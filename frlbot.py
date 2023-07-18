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
# Import gpt4free class
import g4f

# Specify logging level
logging.basicConfig(level=logging.INFO)

# Get bot token from ENV
def initializeBot() -> str:
    # Read API Token from environment variables
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
    return BOT_TOKEN

# Get target chat from ENVimport datetime
def getTargetChatId() -> int:
    # Read API Token from environment variables
    BOT_TARGET: str = os.environ.get('BOT_TARGET')
    if (not BOT_TARGET):
        logging.critical("Input token is empty!")
        raise Exception("Invalid BOT_TARGET")
    if (len(BOT_TARGET) < 10):
        logging.critical("Input token is too short!")
        raise Exception("Invalid BOT_TARGET")
    # Return token
    return BOT_TARGET

# Get how many news we should post at each loop
def getMaxNewsCnt() -> int:
    return os.getenv('NEWS_COUNT', default=5)

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
        self.author = inputAuthor.strip().title()
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
def parseNews() -> list[newsFromFeed]:
    urls = [
                'https://www.amsat.org/feed/',
                'https://qrper.com/feed/',
                'https://swling.com/blog/feed/', 
                'https://www.ari.it/?format=feed&type=rss',
                'https://www.cisar.it/index.php?format=feed&type=rss',
                'https://www.blogger.com/feeds/3151423644013078076/posts/default',
                'https://www.pa9x.com/feed/',
                'https://www.ham-yota.com/feed/'
            ]

    fetchFeed = [feedparser.parse(url)['entries'] for url in urls]
    feedsList = [item for feed in fetchFeed for item in feed]
    feedsList.sort(key=lambda x: dateutil.parser.parse(x['published']), reverse=True)
    # Prepare list of news
    newsList: list[newsFromFeed] = []
    # Scan each feed and convert it to a class element. Store the checksum to avoid dupes
    for singleFeed in feedsList:
        newArticle = newsFromFeed(singleFeed["title"], singleFeed["published"], singleFeed["author"], singleFeed["summary"], singleFeed["link"])
        newsList.append(newArticle)
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
    
def Main():
    logging.info("Starting bot")
    # Connect to SQLite
    logging.info("Opening SQLite store")
    con = sqlite3.connect("frlbot.db")
    cur = con.cursor()
    try:
        cur.execute("CREATE TABLE news(date, checksum)")
    except:
        logging.debug("Table already exists")
    # Generate bot object
    bot = telebot.TeleBot(initializeBot())
    # Track how many news we sent
    newsCnt: int = 0
    maxNews = getMaxNewsCnt()
    # Get news from feed
    for singleNews in parseNews():
        # Check if we already sent this message
        if cur.execute("SELECT * FROM news WHERE checksum='" + singleNews.checksum + "'").fetchone() is None:
            logging.info("Sending: [" + singleNews.link + "]")
            # Prepare message to send
            try:
                msgToSend = "\U0001F4E1 " + singleNews.title + \
                            "\n\n\U0000270F Autore: " + singleNews.author + \
                            "\n\U0001F4C5 Data: " + singleNews.date.strftime("%Y/%m/%d, %H:%M") + \
                            "\n\n" + ReworkText(singleNews) + \
                            "\n\n\U0001F517 Articolo completo: " + singleNews.link
                bot.send_message(getTargetChatId(), msgToSend, parse_mode="MARKDOWN")
                # Store this article to DB
                logging.debug("Adding [" + singleNews.checksum + "] to store")
                cur.execute("INSERT INTO news(date, checksum) VALUES(?, ?)", [singleNews.date, singleNews.checksum])
                con.commit()
                newsCnt += 1
            except Exception as retExc:
                logging.error(str(retExc))
        # This message was already posted
        else:
            logging.warning("Post at [" + singleNews.link + "] was already sent")
        # Stop execution after sending x elements
        if newsCnt >= maxNews:
            break

schedule.every().day.at("07:00").do(Main, )
schedule.every().day.at("19:00").do(Main, )

# Main method invocation
if __name__ == "__main__":
    logging.info("Starting frlbot at " + str(datetime.now()))
    while True:
        schedule.run_pending()
        logging.debug("Waiting...")
        time.sleep(50) # wait one minute