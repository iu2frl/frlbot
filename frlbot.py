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
from datetime import datetime, timedelta
import re
import hashlib
import sqlite3
import schedule
import time
import sys
import getopt
import threading
from googletrans import Translator
import requests
import xml.dom.minidom
import emoji
import requests

# Specify logging level
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('hpack').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.INFO)

# Set DryRun mode
dryRun = False
forceRun = False
noAi = False

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
        return -1
    # Read API Token from environment variables
    BOT_TARGET: str = os.environ.get('BOT_ADMIN')
    if (not BOT_TARGET):
        logging.warning("Admin is empty! No commands will be accepted")
        return -1
    # Return token
    return int(BOT_TARGET)

# Get maximum news age
def getMaxNewsDays() -> int:
    if dryRun:
        return 30
    # Read API Token from environment variables
    return int(os.getenv('MAX_NEWS_AGE', default=30))

# Get how many news we should post at each loop
def getMaxNewsCnt() -> int:
    return int(os.getenv('NEWS_COUNT', default=1))

# Get post send interval
def getPostInterval() -> int:
    return int(os.getenv('POST_INTERVAL', default=41))

# Bot initialization
def InitializeBot():
    global telegramBot
    telegramBot = telebot.TeleBot(GetBotApiKey())

# Remove HTML code
def RemoveHtml(inputText: str) -> str:
    regExHtml = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
    return re.sub(regExHtml, "", inputText.strip())

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
        self.date = dateutil.parser.parse(inputDate).replace(tzinfo=None)
        self.author = inputAuthor.strip()
        if len(inputSummary) > 10:
            # Remove "Read more"
            regExReadMore = re.compile(re.escape("read more"), re.IGNORECASE)
            # Parse summary
            noReadMore = re.sub(regExReadMore, "", inputSummary.strip())
        else:
            noReadMore = inputSummary
        # Cut input text
        if len(noReadMore) > 300:
            cutText: str = noReadMore[:300] + " ..."
        else:
            cutText: str = noReadMore
        self.summary = cutText.strip()
        cleanLink = inputLink.strip().lower()
        self.link = "[" + self.title + "](" + cleanLink + ")"
        # Calculate checksum
        self.checksum = hashlib.md5(cleanLink.encode('utf-8')).hexdigest()
        pass

# Extract domain from URL
def extract_domain(url):
    pattern = r'https?://(?:www\.)?([^/]+)'
    result = re.match(pattern, url)
    if result:
        return result.group(1)
    return "anonymous"

# Parse RSS feed
def parseNews(urlsList: list[str]) -> list[newsFromFeed]:
    # Get feeds from the list above
    fetchFeed = []
    for url in urlsList:
        logging.debug("Retrieving feed at [" + url + "]")
        r: requests.Response()
        try:
            r = requests.get(url, timeout=3)
        except Exception as retExc:
            logging.error("Cannot download feed from [" + url + "]. Error message: " + str(retExc))
        if r.status_code == 200:
            try:
                fetchFeed.append(feedparser.parse(r.content)["entries"])
            except Exception as retExc:
                logging.error("Cannot parse feed from [" + url + "]. Error message: " + str(retExc))
        else:
            logging.warning("Got error code [" + str(r.status_code) + "] while retrieving content at [" + url + "]")
    feedsList = [item for feed in fetchFeed for item in feed]
    # Prepare list of news
    newsList: list[newsFromFeed] = []
    # Scan each feed and convert it to a class element. Store the checksum to avoid dupes
    for singleFeed in feedsList:
        logging.debug("Processing [" + singleFeed["link"] + "]")
        # Old RSS format
        if singleFeed["summary"]:
            newsContent = RemoveHtml(singleFeed["summary"])
            # Check if valid content
            if len(newsContent) <= 10:
                logging.warning("Skipping [" + singleFeed["link"] + "], empty content")   
                continue
            # Generate new article
            try:
                if singleFeed["author"]:
                    newArticle = newsFromFeed(singleFeed["title"], singleFeed["published"], singleFeed["author"], newsContent, singleFeed["link"])
                else:
                    newArticle = newsFromFeed(singleFeed["title"], singleFeed["published"], extract_domain(singleFeed["link"]), newsContent, singleFeed["link"])
                newsList.append(newArticle)
            except Exception as retExc:
                logging.warning("Cannot process [" + singleFeed["link"] + "], exception: " + str(retExc))
        # New RSS format
        elif singleFeed["description"]:
            # Check if valid content
            newsContent = RemoveHtml(singleFeed["description"])
            # Check if valid content
            if len(newsContent) <= 10:
                logging.warning("Skipping [" + singleFeed["link"] + "], empty content")   
                continue
            # Generate new article
            try:
                if singleFeed["dc:creator"]:
                    newArticle = newsFromFeed(singleFeed["title"], singleFeed["pubDate"], singleFeed["dc:creator"], newsContent, singleFeed["link"])
                else:
                    newArticle = newsFromFeed(singleFeed["title"], singleFeed["pubDate"], extract_domain(singleFeed["link"]), newsContent, singleFeed["link"])
            except Exception as retExc:
                logging.warning("Cannot process [" + singleFeed["link"] + "], exception: " + str(retExc))
            newsList.append(newArticle)
        else:
            # Unknown format
            logging.warning("Skipping [" + singleFeed["link"] + "], incompatible RSS format")
            continue
    # Return list
    logging.info("Fetch [" + str(len(newsList)) + "] news")
    newsList.sort(key=lambda news: news.date, reverse=True)
    return newsList

# Handle translation
def TranslateText(inputText: str, destLang: str = "it") -> str:
    # Check if skip translations
    if noAi:
        return inputText
    # Start text rework
    logging.debug("Translating: [" + inputText + "]")
    translator = Translator()
    trResponse = None
    try:
        trResponse = translator.translate(inputText, dest=destLang)
    except Exception as retExc:
        logging.error(str(retExc))
        return inputText
    logging.debug(trResponse)
    if trResponse is None:
        logging.error("Unable to translate text")
        return inputText
    elif len(trResponse.text) < 10:
        logging.error("Translation was too short")
        return inputText
    return trResponse.text
    
# Database preparation
def PrepareDb() -> None:
    # Connect to SQLite
    logging.debug("Opening SQLite store")
    sqliteConn = GetSqlConn()
    sqliteCursor = sqliteConn.cursor()
    # Create news table
    try:
        sqliteCursor.execute("CREATE TABLE news(date, checksum)")
        logging.info("News table was generated successfully")
    except:
        logging.debug("News table already exists")
    # Count sent articles
    try:
        dataFromDb = sqliteCursor.execute("SELECT checksum FROM news WHERE 1").fetchall()
        logging.info("News table contains [" + str(len(dataFromDb)) + "] records")
    except Exception as retExc:
        logging.critical("Error while getting count of news records: " + str(retExc))
        raise Exception(retExc)
    # Create feeds table
    try:
        sqliteCursor.execute("CREATE TABLE feeds(url)")
        logging.info("Feeds table was generated successfully")
    except:
        logging.debug("Feeds table already exists")
    # Get feeds from DB
    dataFromDb = sqliteCursor.execute("SELECT url FROM feeds WHERE 1").fetchall()
    if (len(dataFromDb) < 1):
        logging.info("News table is empty, adding default")
        try:
            for singleUrl in defaultUrls:
                logging.debug("Adding [" + singleUrl + "]")
                sqliteCursor.execute("INSERT INTO feeds(url) VALUES(?)", [singleUrl])
            sqliteConn.commit()
            if (len(sqliteCursor.execute("SELECT url FROM feeds WHERE 1").fetchall()) < 1):
                raise Exception("Records were not added!")
            logging.debug("Default records were added")
        except Exception as retExc:
            logging.error(retExc)
            return
    else:
        logging.info("Feeds table contains [" + str(len(dataFromDb)) + "] records")
    # Close DB connection
    sqliteConn.close()

# Get SQL Connector
def GetSqlConn() -> sqlite3.Connection:
    return sqlite3.connect("store/frlbot.db", timeout=3)

# Delete old SQLite records
def RemoveOldNews(max_days: int = -1) -> int:
    if max_days == -1:
        max_days = getMaxNewsDays()
    try:
        # Get SQL cursor
        sqlCon = GetSqlConn()
        oldNews = sqlCon.cursor().execute("SELECT date FROM news WHERE date <= date('now', '-" + str(max_days) + " day')").fetchall()
        logging.info("Removing [" + str(len(oldNews)) + "] old news from DB")
        sqlCon.cursor().execute("DELETE FROM news WHERE date <= date('now', '-" + str(max_days) + " day')")
        sqlCon.commit()
        sqlCon.close()
        return len(oldNews)
    except Exception as retExc:
        logging.error("Cannot delete older news. " + str(retExc))
        return -1

# Main code
def Main():
    logging.info("Starting bot")
    # Generate bot object
    global telegramBot
    # Track how many news we sent
    newsCnt: int = 0
    maxNews = getMaxNewsCnt()
    # Get SQL cursor
    sqlCon = GetSqlConn()
    # Clean data from DB
    feedsFromDb = [x[0] for x in sqlCon.cursor().execute("SELECT url FROM feeds WHERE 1").fetchall()]
    if feedsFromDb is None:
        logging.error("No news from DB")
        sqlCon.close()
        return
    logging.debug("Fetching [" + str(len(feedsFromDb)) + "] feeds")
    # Monitor exceptions and report in case of multiple errors
    excCnt = 0
    excMsg = ""
    # Get news from feed
    for singleNews in parseNews(feedsFromDb):
        # Check if we already sent this message
        if sqlCon.cursor().execute("SELECT * FROM news WHERE checksum='" + singleNews.checksum + "'").fetchone() is None:
            logging.info("Sending: [" + singleNews.link + "]")
            # Check if article is no more than 30 days
            if datetime.now().replace(tzinfo=None) - singleNews.date.replace(tzinfo=None) > timedelta(days=30):
                logging.debug("Article: [" + singleNews.link + "] is older than 30 days, skipping")
            elif singleNews.date.replace(tzinfo=None) > datetime.now().replace(tzinfo=None):
                logging.warning("Article: [" + singleNews.link + "] is coming from the future?!")
            else:
                # Prepare message to send
                itFlagEmoji = emoji.emojize(":Italy:", language="alias")
                enFlagEmoji = emoji.emojize(":United_States:", language="alias")
                autEmojy = emoji.emojize(":pencil2:", language="alias")
                calEmoji = emoji.emojize(":spiral_calendar:", language="alias")
                linkEmoji = emoji.emojize(":link:", language="alias")
                try:
                    msgToSend = f"{itFlagEmoji} {TranslateText(singleNews.title, 'it')}\n" + \
                                f"{enFlagEmoji} {TranslateText(singleNews.title, 'en')}\n" + \
                                f"\n{autEmojy} {singleNews.author}\n" + \
                                f"{calEmoji} {singleNews.date.strftime('%Y/%m/%d, %H:%M')}\n" + \
                                f"\n{itFlagEmoji} {TranslateText(singleNews.summary, 'it')}\n" + \
                                f"\n{enFlagEmoji} {TranslateText(singleNews.summary, 'en')}\n" + \
                                f"\n{linkEmoji} {singleNews.link}"
                    if not dryRun:
                        telegramBot.send_message(getTargetChatId(), msgToSend, parse_mode="MARKDOWN")
                    else:
                        logging.info(msgToSend)
                    if not dryRun:
                        # Store this article to DB
                        logging.debug("Adding [" + singleNews.checksum + "] to store")
                        sqlCon.cursor().execute("INSERT INTO news(date, checksum) VALUES(?, ?)", [singleNews.date, singleNews.checksum])
                        sqlCon.commit()
                    newsCnt += 1
                except Exception as retExc:
                    logging.error(str(retExc))
                    excCnt += 1
                    excMsg = str(retExc)
        # This message was already posted
        else:
            logging.debug("Post at [" + singleNews.link + "] was already sent")
        # Check errors count
        if excCnt > 3:
            logging.error("Too many errors, skipping this upgrade")
            if not dryRun:
                telegramBot.send_message(getAdminChatId(), "Too many errors, skipping this execution. Last error: `" + excMsg + "`")
            break
        # Stop execution after sending x elements
        if newsCnt >= maxNews:
            break
    logging.debug("No more articles to process, waiting for next execution")
    # Close DB connection
    sqlCon.close()

# Check if force send
def CheckArgs(argv) -> list[bool, bool, bool]:
    try:
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
    except:
        return None
# Check if valid XML
def ValidXml(inputUrl: str) -> bool:
    try:
        getRes = requests.get(inputUrl)
        xml.dom.minidom.parseString(getRes.content)
        return True
    except:
        return False

# Cleanup old news
schedule.every().day.at("01:00").do(RemoveOldNews, )
# Execute bot news
schedule.every(getPostInterval()).minutes.do(Main, )

def SchedulerLoop():
    logging.info("Starting scheduler loop")
    while True:
        schedule.run_pending()
        time.sleep(5)

def TelegramLoop():
    logging.info("Starting telegram loop")
    telegramBot.infinity_polling()

# Main method invocation
if __name__ == "__main__":
    logging.info("Starting frlbot at " + str(datetime.now()))
    # Check if store folder exists
    if not os.path.exists("store"):
        logging.info("Creating 'store' folder")
        os.makedirs("store")
    # Check if script was forcefully run
    try:
        dryRun, forceRun, noAi = CheckArgs(sys.argv[1:])
    except:
        logging.critical("Invalid command line arguments have been set")
        exit()
    # Initialize Bot
    if not dryRun:
        InitializeBot()
        # Handle LIST command
        @telegramBot.message_handler(content_types=["text"], commands=['urllist'])
        def HandleUrlListMessage(inputMessage: telebot.types.Message):
            if inputMessage.from_user.id == getAdminChatId():
                logging.debug("URL list requested from [" + str(inputMessage.from_user.id) + "]")
                global telegramBot
                sqlCon = GetSqlConn()
                feedsFromDb = [(x[0], x[1]) for x in sqlCon.cursor().execute("SELECT rowid, url FROM feeds WHERE 1").fetchall()]
                sqlCon.close()
                if len(feedsFromDb) < 1:
                    telegramBot.reply_to(inputMessage, "No URLs in the url table")
                else:
                    textMessage: str = ""
                    for singleElement in feedsFromDb:
                        # Check if message is longer than max length
                        if len(textMessage) + len(singleElement[1]) + 10 >= 4096:
                            telegramBot.send_message(inputMessage.from_user.id, textMessage)
                            textMessage = ""
                        textMessage += str(singleElement[0]) + ": " + singleElement[1] + "\n"
                    telegramBot.send_message(inputMessage.from_user.id, textMessage)
            else:
                logging.debug("Ignoring [" + inputMessage.text + "] message from [" + str(inputMessage.from_user.id) + "]")
        # Add new feed to the store   
        @telegramBot.message_handler(content_types=["text"], commands=['addfeed'])
        def HandleAddMessage(inputMessage: telebot.types.Message):
            if inputMessage.from_user.id == getAdminChatId():
                global telegramBot
                sqlCon = GetSqlConn()
                splitText = inputMessage.text.split(" ")
                if (len(splitText) == 2):
                    # Check if URL is valid
                    if "http" not in splitText[1]:
                        logging.warning("Invalid URL [" + splitText[1] + "]")
                        telegramBot.reply_to(inputMessage, "Invalid URL format")
                        return
                    logging.debug("Feed add requested from [" + str(inputMessage.from_user.id) + "]")
                    # Check if feed already exists
                    if sqlCon.execute("SELECT * FROM feeds WHERE url=?", [splitText[1]]).fetchone() is not None:
                        logging.warning("Duplicate URL [" + splitText[1] + "]")
                        telegramBot.reply_to(inputMessage, "URL exists in the DB")
                        return
                    # Add it to the store
                    try:
                        logging.info("Adding [" + splitText[1] + "] to DB")
                        if ValidXml(splitText[1]):
                            sqlCon.execute("INSERT INTO feeds(url) VALUES(?)", [splitText[1]])
                            sqlCon.commit()
                            telegramBot.reply_to(inputMessage, "Added successfully!")
                        else:
                            telegramBot.reply_to(inputMessage, "RSS feed cannot be validated (invalid syntax or unreachable)")
                    except Exception as retExc:
                        telegramBot.reply_to(inputMessage, retExc)
                else:
                    logging.warning("Invalid AddFeed arguments [" + inputMessage.text + "]")
                    telegramBot.reply_to(inputMessage, "Expecting only one argument")
            else:
                logging.debug("Ignoring [" + inputMessage.text + "] message from [" + str(inputMessage.from_user.id) + "]")
            # Close DB connection
            sqlCon.close()
        # Remove feed from the stores
        @telegramBot.message_handler(content_types=["text"], commands=['rmfeed'])
        def HandleRemoveMessage(inputMessage: telebot.types.Message):
            if inputMessage.from_user.id == getAdminChatId():
                global telegramBot
                sqlCon = GetSqlConn()
                splitText = inputMessage.text.split(" ")
                if (len(splitText) == 2):
                    if (splitText[1].isnumeric()):
                        logging.debug("Feed deletion requested from [" + str(inputMessage.from_user.id) + "]")
                        try:
                            sqlCon.execute("DELETE FROM feeds WHERE rowid=?", [splitText[1]])
                            sqlCon.commit()
                            sqlCon.close()
                            telegramBot.reply_to(inputMessage, "Element was removed successfully!")
                        except Exception as retExc:
                            telegramBot.reply_to(inputMessage, retExc)
                    else:
                        telegramBot.reply_to(inputMessage, "[" + splitText[1] +"] is not a valid numeric index")
                else:
                    telegramBot.reply_to(inputMessage, "Expecting only one argument")
            else:
                logging.debug("Ignoring [" + inputMessage.text + "] message from [" + str(inputMessage.from_user.id) + "]")
        # Force bot execution
        @telegramBot.message_handler(content_types=["text"], commands=['force'])
        def HandleForceMessage(inputMessage: telebot.types.Message):
            if inputMessage.from_user.id == getAdminChatId():
                logging.debug("Manual bot execution requested from [" + str(inputMessage.from_user.id) + "]")
                global telegramBot
                telegramBot.reply_to(inputMessage, "Forcing bot execution")
                Main()
            else:
                logging.debug("Ignoring [" + inputMessage.text + "] message from [" + str(inputMessage.from_user.id) + "]")
        # Remove old news
        @telegramBot.message_handler(content_types=["text"], commands=['rmoldnews'])
        def HandleOldNewsDelete(inputMessage: telebot.types.Message):
            if inputMessage.from_user.id == getAdminChatId():
                logging.debug("Manual news deletion requested from [" + str(inputMessage.from_user.id) + "]")
                global telegramBot
                splitMessage = inputMessage.text.split(" ")
                if len(splitMessage) != 2:
                    telegramBot.reply_to(inputMessage, "Expecting only one argument")
                elif splitMessage[1].isdigit():
                    deletedNews = RemoveOldNews(int(splitMessage[1]))
                    if deletedNews >= 0:
                        telegramBot.reply_to(inputMessage, "Deleting [" + str(deletedNews) + "] news older than [" + str(splitMessage[1]) + "] days")
                    else:
                        telegramBot.reply_to(inputMessage, "Cannot delete older news, check log for error details")
                else:
                    telegramBot.reply_to(inputMessage,"Invalid number of days to delete")
            else:
                logging.debug("Ignoring message from [" + str(inputMessage.from_user.id) + "]")
        # Add from CSV list
        @telegramBot.message_handler(content_types=["text"], commands=['addcsv'])
        def HandleAddCsvList(inputMessage: telebot.types.Message):
            if inputMessage.from_user.id == getAdminChatId():
                logging.debug("Adding news from CSV list")
                global telegramBot
                sqlCon = GetSqlConn()
                splitMessage = inputMessage.text.split("/addcsv")
                # Invalid syntax
                if len(splitMessage) <= 1:
                    telegramBot.reply_to(inputMessage, "Missing CSV list")
                    return
                splitCsv = splitMessage[1].split(",")
                # Not enough elements
                if len(splitCsv) <= 1:
                    telegramBot.reply_to(inputMessage, "Expecting more than 1 value in CSV format")
                    return
                telegramBot.reply_to(inputMessage, "Processing, please be patient...")
                newFeedsCnt = 0
                for singleUrl in splitCsv:
                    # Clean input string
                    singleUrl = singleUrl.strip()
                    # Check if feed already exists
                    if sqlCon.execute("SELECT * FROM feeds WHERE url=?", [singleUrl]).fetchone() is not None:
                        logging.warning("Duplicate URL [" + singleUrl + "]")
                    else:
                        try:
                            logging.info("Adding [" + singleUrl + "] to DB")
                            if ValidXml(singleUrl):
                                sqlCon.execute("INSERT INTO feeds(url) VALUES(?)", [singleUrl])
                                newFeedsCnt += 1
                                logging.debug("Added [" + singleUrl + "] to DB")
                            else:
                                logging.warning("RSS feed [" + singleUrl + "] cannot be validated")
                        except Exception as retExc:
                            continue
                # Commit changes to DB
                sqlCon.commit()
                sqlCon.close()
                # Send reply
                telegramBot.reply_to(inputMessage, "[" + str(newFeedsCnt) + "] out of [" + str(len(splitCsv)) + "] feeds were added to DB")
            else:
                logging.debug("Ignoring message from [" + str(inputMessage.from_user.id) + "]")
        # Perform DB cleanup (duplicate and invalid)
        @telegramBot.message_handler(content_types=["text"], commands=['dbcleanup'])
        def HandleDbCleanup(inputMessage: telebot.types.Message):
            if inputMessage.from_user.id == getAdminChatId():
                logging.debug("Peforming news cleanup")
                global telegramBot
                telegramBot.reply_to(inputMessage, "Performing cleanup, please be patient...")
                sqlCon = GetSqlConn()
                feedsFromDb = [(x[0], x[1]) for x in sqlCon.cursor().execute("SELECT rowid, url FROM feeds WHERE 1").fetchall()]
                duplicatesCnt = 0
                invalidsCnt = 0
                for singleElement in feedsFromDb:
                    cleanUrl = singleElement[1].split("://")[1].replace("www.", "")
                    logging.debug("Checking for duplicate [" + cleanUrl + "]")
                    # Query to check for duplicate URLs with a different rowid
                    query = "SELECT rowid FROM feeds WHERE url LIKE ? AND rowid != ?"
                    # Execute the query
                    duplicates = sqlCon.cursor().execute(query, ("%" + cleanUrl + "%", singleElement[0])).fetchall()
                    if duplicates:
                        # Remove duplicate
                        logging.info("Removing duplicate [" + singleElement[1] + "] from DB")
                        sqlCon.execute("DELETE FROM feeds WHERE rowid=?", [singleElement[0]])
                        sqlCon.commit()
                        duplicatesCnt += 1
                    else:
                        # Check if feed is valid
                        if not ValidXml(singleElement[1]):
                            # Remove duplicate
                            logging.info("Removing invalid [" + singleElement[1] + "] from DB")
                            sqlCon.execute("DELETE FROM feeds WHERE rowid=?", [singleElement[0]])
                            sqlCon.commit()
                            invalidsCnt += 1
                # Close DB connection
                sqlCon.close()
                # Return output
                telegramBot.reply_to(inputMessage, "Removed [" + str(invalidsCnt) + "] invalid and [" + str(duplicatesCnt) + "] duplicated RSS feeds")
            else:
                logging.debug("Ignoring message from [" + str(inputMessage.from_user.id) + "]")
        # Perform DB backup
        @telegramBot.message_handler(content_types=["text"], commands=['sqlitebackup'])
        def HandleSqliteBackup(inputMessage: telebot.types.Message):
            if inputMessage.from_user.id == getAdminChatId():
                logging.debug("Manual DB backup requested from [" + str(inputMessage.from_user.id) + "]")
                global telegramBot
                try:
                    dbFile = open("store/frlbot.db", "rb")
                    telegramBot.send_document(chat_id=inputMessage.chat.id,
                                            document=dbFile,
                                            reply_to_message_id=inputMessage.id,
                                            caption="SQLite backup at " + str(datetime.now()))
                except Exception as retExc:
                    telegramBot.reply_to(inputMessage, "Error: " + str(retExc))
            else:
                logging.debug("Ignoring message from [" + str(inputMessage.from_user.id) + "]")
    # Prepare DB object
    PrepareDb()
    if forceRun:
        logging.info("Starting forced execution")
        Main()
        sys.exit(0)
    # Start async execution
    logging.info("Starting main loop")
    if not dryRun:
        telegramThread = threading.Thread(target=TelegramLoop, name="TelegramLoop")
        telegramThread.start()
        SchedulerLoop()
    else:
        Main()