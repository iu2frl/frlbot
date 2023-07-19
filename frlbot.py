# TODO:
# - Set BOT_TOKEN to ENV with the API_KEY for the bot
# - Set BOT_TARGET to ENV with the target channel (to send messages)
# - Set NEWS_COUNT if needed

# Import external classes
import feedparser
import dateutil.parser
import telebot
import logging
import os
from datetime import datetime
import re
import hashlib
import sqlite3
import schedule
import time
import sys
import getopt
import asyncio
# Import gpt4free class
import g4f

# Specify logging level
logging.basicConfig(level=logging.DEBUG)

# Set DryRun mode
dryRun = False
forceRun = False
noAi = False

# SQLite stuff
sqliteConn: sqlite3.Connection = None
sqliteCursor: sqlite3.Cursor = None

# Telegram Bot
telegramBot: telebot.TeleBot

# Default feeds
defaultUrls = [
                'https://www.amsat.org/feed/',
                'https://qrper.com/feed/',
                'https://swling.com/blog/feed/', 
                'https://www.ari.it/?format=feed&type=rss',
                'https://www.cisar.it/index.php?format=feed&type=rss',
                'https://www.blogger.com/feeds/3151423644013078076/posts/default',
                'https://www.pa9x.com/feed/',
                'https://www.ham-yota.com/feed/',
                'https://www.iu2frl.it/feed/',
                'https://www.yota-italia.it/feed/',
                'https://feeds.feedburner.com/OnAllBands',
                'https://www.hamradio.me/feed',
            ]

# Get bot token from ENV
def GetBotApiKey() -> str:
    # Read API Token from environment variables
    if dryRun:
        return ""
    BOT_TOKEN: str = os.environ.get('BOT_TOKEN')
    if (not BOT_TOKEN):
        logging.critical("Input token is empty!")
        raise Exception("Invalid BOT_TOKEN")
    if (len(BOT_TOKEN) < 10):
        logging.critical("Input token is too short!")
        raise Exception("Invalid BOT_TOKEN")
    if (":" not in BOT_TOKEN):
        logging.critical("Invalid input token format")
        raise Exception("Invalid BOT_TOKEN")
    # Return token
    return str(BOT_TOKEN)

# Get target chat from ENV
def getTargetChatId() -> int:
    if dryRun:
        return ""
    # Read API Token from environment variables
    BOT_TARGET: str = os.environ.get('BOT_TARGET')
    if (not BOT_TARGET):
        logging.critical("Input token is empty!")
        raise Exception("Invalid BOT_TARGET")
    if (len(BOT_TARGET) < 5):
        logging.critical("Input token is too short!")
        raise Exception("Invalid BOT_TARGET")
    # Return token
    return int(BOT_TARGET)

# Get target chat from ENV
def getAdminChatId() -> int:
    if dryRun:
        return ""
    # Read API Token from environment variables
    BOT_TARGET: str = os.environ.get('BOT_ADMIN')
    if (not BOT_TARGET):
        logging.warning("Admin is empty! No commands will be accepted")
        return -1
    # Return token
    return int(BOT_TARGET)

# Get how many news we should post at each loop
def getMaxNewsCnt() -> int:
    return int(os.getenv('NEWS_COUNT', default=2))

# Bot initialization
def InitializeBot():
    global telegramBot
    telegramBot = telebot.TeleBot(GetBotApiKey())

# Create news class
class newsFromFeed(list):
    title: str = ""
    date: datetime
    author: str = ""
    summary: str = ""
    link: str = ""
    checksum: str = ""

    def __init__(self, inputTitle: str, inputDate: str, inputAuthor: str, inputSummary: str, inputLink: str = "") -> None:
        self.title = inputTitle.strip()
        self.date = dateutil.parser.parse(inputDate)
        self.author = inputAuthor.strip()
        # Remove HTML tags
        regExHtml = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
        # Remove "Read more"
        regExReadMore = re.compile(re.escape("read more"), re.IGNORECASE)
        # Parse summary
        self.summary = re.sub(regExHtml, "", re.sub(regExReadMore, "", inputSummary.strip()))
        self.link = inputLink.strip().lower()
        # Calculate checksum
        self.checksum = hashlib.md5(inputLink.encode('utf-8')).hexdigest()
        pass

# Parse RSS feed
def parseNews(urlsList: list[str]) -> list[newsFromFeed]:
    # Get feeds from the list above
    fetchFeed = [feedparser.parse(url)['entries'] for url in urlsList]
    feedsList = [item for feed in fetchFeed for item in feed]
    # Prepare list of news
    newsList: list[newsFromFeed] = []
    # Scan each feed and convert it to a class element. Store the checksum to avoid dupes
    for singleFeed in feedsList:
        logging.debug("Processing [" + singleFeed["link"] + "]")
        # Old RSS format
        if singleFeed["published"]:
            # Check if valid content
            if len(singleFeed["summary"]) <= 10:
                logging.warning("Skipping [" + singleFeed["link"] + "], empty content")   
                continue
            # Generate new article
            newArticle = newsFromFeed(singleFeed["title"], singleFeed["published"], singleFeed["author"], singleFeed["summary"], singleFeed["link"])
        # New RSS format
        elif singleFeed["pubDate"]:
            # Check if valid content
            if len(singleFeed["description"]) <= 10:
                logging.warning("Skipping [" + singleFeed["link"] + "], empty content")
                continue
            # Generate new article
            newArticle = newsFromFeed(singleFeed["title"], singleFeed["pubDate"], singleFeed["dc:creator"], singleFeed["description"], singleFeed["link"])
        else:
            # Unknown format
            logging.warning("Skipping [" + singleFeed["link"] + "], incompatible RSS format")
            continue
        newsList.append(newArticle)
    # Return list
    logging.info("Fetch [" + str(len(newsList)) + "] news")
    newsList.sort(key=lambda news: news.date, reverse=True)
    return newsList

# Loop per each AI provider
def HandleAi(inputQuery: str, botProvider) -> str:
    try:
        gptResponse: str = g4f.ChatCompletion.create(model="gpt-3.5-turbo", provider=botProvider, messages=[{"role": "user", "content": inputQuery}])
        if len(gptResponse) >= 10:
            logging.debug("Response: " + gptResponse)
    except Exception as retExc:
        logging.error(str(retExc))
        gptResponse = ""
    finally:
        return gptResponse

# Handle GPT stuff
def ReworkText(inputNews: newsFromFeed) -> str:
    # Check if skip AI
    if noAi:
        return inputNews.summary
    # Start AI rework
    logging.debug("Reworking: [" + inputNews.link + "]")
    gptCommand = "Rielabora questo testo e traducilo in italiano se necessario: "
    inputQuery = gptCommand + inputNews.summary
    validResult = False
    providersList = [g4f.Provider.GetGpt, g4f.Provider.DeepAi, g4f.Provider.Aichat]
    for singleProvider in providersList:
        gptResponse = HandleAi(inputQuery, singleProvider)
        if len(gptResponse) > 10:
            # Cleanup response from GPT if needed
            regExQuery = re.compile(re.escape(gptCommand), re.IGNORECASE)
            gptResponse = re.sub(r"(\[\^\d\^\])", "", re.sub(regExQuery, "", gptResponse))
            return gptResponse
    # If none have worked, return original text
    if not validResult:
        logging.error("Unable to process AI text rework")
        return inputNews.summary
    
# Database preparation
def PrepareDb() -> None:
    # Connect to SQLite
    logging.debug("Opening SQLite store")
    global sqliteConn 
    global sqliteCursor
    sqliteConn = sqlite3.connect("store/frlbot.db")
    sqliteCursor = sqliteConn.cursor()
    # Create news table
    try:
        sqliteCursor.execute("CREATE TABLE news(date, checksum)")
        logging.info("News table was generated successfully")
    except:
        logging.debug("News table already exists")
    # Create feeds table
    try:
        sqliteCursor.execute("CREATE TABLE feeds(url)")
        logging.info("Feeds table was generated successfully")
    except:
        logging.debug("Feeds table already exists")

# Main code
def Main():
    logging.info("Starting bot")
    global sqliteConn
    global sqliteCursor
    # Generate bot object
    global telegramBot
    # Track how many news we sent
    newsCnt: int = 0
    maxNews = getMaxNewsCnt()
    # Get feeds from DB
    if (sqliteCursor.execute("SELECT url FROM feeds WHERE 1").fetchone() is None):
        logging.info("News table is empty, adding default")
        try:
            for singleUrl in defaultUrls:
                sqliteCursor.execute("INSERT INTO feeds(url) VALUES(?)", [singleUrl])
            sqliteConn.commit()
            logging.debug("Default records were added")
        except Exception as retExc:
            logging.error(retExc)
            return
    # Clean data from DB
    feedsFromDb = [x[0] for x in sqliteCursor.execute("SELECT url FROM feeds WHERE 1").fetchall()]
    if feedsFromDb is None:
        logging.error("No news from DB")
        return
    logging.debug("Fetch: " + str(feedsFromDb))
    # Get news from feed
    for singleNews in parseNews(feedsFromDb):
        # Check if we already sent this message
        if sqliteCursor.execute("SELECT * FROM news WHERE checksum='" + singleNews.checksum + "'").fetchone() is None:
            logging.info("Sending: [" + singleNews.link + "]")
            # Prepare message to send
            try:
                msgToSend = "\U0001F4E1 " + singleNews.title + \
                            "\n\n\U0000270F Autore: " + singleNews.author + \
                            "\n\U0001F4C5 Data: " + singleNews.date.strftime("%Y/%m/%d, %H:%M") + \
                            "\n\n" + ReworkText(singleNews) + \
                            "\n\n\U0001F517 Articolo completo: " + singleNews.link
                if not dryRun:
                    telegramBot.send_message(getTargetChatId(), msgToSend, parse_mode="MARKDOWN")
                else:
                    logging.info(msgToSend)
                if not dryRun:
                    # Store this article to DB
                    logging.debug("Adding [" + singleNews.checksum + "] to store")
                    sqliteCursor.execute("INSERT INTO news(date, checksum) VALUES(?, ?)", [singleNews.date, singleNews.checksum])
                    sqliteConn.commit()
                newsCnt += 1
            except Exception as retExc:
                logging.error(str(retExc))
        # This message was already posted
        else:
            logging.warning("Post at [" + singleNews.link + "] was already sent")
        # Stop execution after sending x elements
        if newsCnt >= maxNews:
            break

# Check if force send
def CheckArgs(argv) -> list[bool, bool, bool]:
    opts, args = getopt.getopt(argv,"fdn",["force", "dry", "noai"])
    dryRun = False
    forceRun = False
    noAi = False
    for opt, arg in opts:
        if opt in ("-d", "--dry"):
            dryRun = True
        if opt in ("-f", "--force"):
            forceRun = True
        if opt in ("-n", "--noai"):
            noAi = True
    logging.info("DryRun: " + str(dryRun) + " - ForceRun: " + str(forceRun) + " - NoAI: " + str(noAi))
    return dryRun, forceRun, noAi

schedule.every().day.at("06:00").do(Main, )
schedule.every().day.at("07:00").do(Main, )
schedule.every().day.at("08:00").do(Main, )
schedule.every().day.at("09:00").do(Main, )
schedule.every().day.at("10:00").do(Main, )
schedule.every().day.at("11:00").do(Main, )
schedule.every().day.at("12:00").do(Main, )
schedule.every().day.at("13:00").do(Main, )
schedule.every().day.at("14:00").do(Main, )
schedule.every().day.at("15:00").do(Main, )
schedule.every().day.at("16:00").do(Main, )
schedule.every().day.at("17:00").do(Main, )
schedule.every().day.at("18:00").do(Main, )
schedule.every().day.at("19:00").do(Main, )
schedule.every().day.at("20:00").do(Main, )
schedule.every().day.at("21:00").do(Main, )
schedule.every().day.at("22:00").do(Main, )

# Main method invocation
if __name__ == "__main__":
    logging.info("Starting frlbot at " + str(datetime.now()))
    # Check if store folder exists
    if not os.path.exists("store"):
        logging.info("Creating 'store' folder")
        os.makedirs("store")
    # Check if script was forcefully run
    dryRun, forceRun, noAi = CheckArgs(sys.argv[1:])
    # Initialize Bot
    if not dryRun:
        InitializeBot()
    # Prepare DB object
    PrepareDb()
    if forceRun:
        logging.info("Starting forced execution")
        Main()
        sys.exit(0)
    # Async bot polling
    asyncio.get_event_loop().run_until_complete(telegramBot.infinity_polling())
    # Scheduled execution
    while True:
        schedule.run_pending()

# Handle LIST command
@telegramBot.message_handler(content_types=["text"], commands=['urllist'])
def HandleUrlListMessage(inputMessage: telebot.types.Message):
    global telegramBot
    if inputMessage.from_user.id == getAdminChatId():
        feedsFromDb = [(x[0], x[1]) for x in sqliteCursor.execute("SELECT rowid, url FROM feeds WHERE 1").fetchall()]
        if feedsFromDb is None:
            telegramBot.reply_to(inputMessage, "No URLs in the url table")
        else:
            textMessage: str = ""
            for singleElement in feedsFromDb:
                textMessage += singleElement[0] + ": " + singleElement[1] + "\n"
            telegramBot.reply_to(inputMessage, textMessage)
