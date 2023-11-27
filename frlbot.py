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
default_urls = [
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
def get_bot_api_from_env() -> str:
    """Return the bot token from environment variables"""
    # Read API Token from environment variables
    if dryRun:
        return ""
    env_token: str = os.environ.get('BOT_TOKEN')
    if (not env_token):
        logging.critical("Input token is empty!")
        raise Exception("Invalid BOT_TOKEN")
    if (len(env_token) < 10):
        logging.critical("Input token is too short!")
        raise Exception("Invalid BOT_TOKEN")
    if (":" not in env_token):
        logging.critical("Invalid input token format")
        raise Exception("Invalid BOT_TOKEN")
    # Return token
    return str(env_token)

# Get target chat from ENV
def get_target_chat_from_env() -> int:
    """Return the target chat ID from environment variables"""
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
def get_admin_chat_from_env() -> int:
    """Return the admin chat ID from environment variables"""
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
def get_max_news_days_from_env() -> int:
    """Return the maximum days a news should be stored from environment variables"""
    if dryRun:
        return 30
    # Read API Token from environment variables
    return int(os.getenv('MAX_NEWS_AGE', default=30))

# Get how many news we should post at each loop
def get_max_news_cnt_from_env() -> int:
    """Return how many news to process from environment variables"""
    return int(os.getenv('NEWS_COUNT', default=1))

# Get post send interval
def get_post_interval_from_env() -> int:
    """Return the publishing interval from environment variables"""
    return int(os.getenv('POST_INTERVAL', default=41))

# Bot initialization
def init_bot():
    """Initialize the Telegram bot class"""
    global telegramBot
    telegramBot = telebot.TeleBot(get_bot_api_from_env())

# Remove HTML code
def remove_html(inputText: str) -> str:
    """Remove html code from the news content"""
    regex_html = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
    return re.sub(regex_html, "", inputText.strip())

# Create news class
class NewsFromFeed(list):
    """Custom class to store news content"""
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
            regex_read_more = re.compile(re.escape("read more"), re.IGNORECASE)
            # Parse summary
            no_read_more = re.sub(regex_read_more, "", inputSummary.strip())
        else:
            no_read_more = inputSummary
        # Cut input text
        if len(no_read_more) > 300:
            cut_text: str = no_read_more[:300] + " ..."
        else:
            cut_text: str = no_read_more
        self.summary = cut_text.strip()
        clean_url = inputLink.strip().lower()
        self.link = "[" + self.title + "](" + clean_url + ")"
        # Calculate checksum
        self.checksum = hashlib.md5(clean_url.encode('utf-8')).hexdigest()

# Extract domain from URL
def extract_domain(url):
    """Extract the domain name from an URL"""
    pattern = r'https?://(?:www\.)?([^/]+)'
    result = re.match(pattern, url)
    if result:
        return result.group(1)
    return "anonymous"

# Parse RSS feed
def parse_news(urls_list: list[str]) -> list[NewsFromFeed]:
    """Reads the url list and returns a list of RSS contents"""
    # Get feeds from the list above
    fetch_feeds = []
    for url in urls_list:
        logging.debug("Retrieving feed at [" + url + "]")
        r: requests.Response = None
        try:
            r = requests.get(url, timeout=10)
        except Exception as ret_exception:
            logging.error("Cannot download feed from [" + url + "]. Error message: " + str(ret_exception))
        if r is None:
            logging.warning(f"Cannot retrieve [{url}] check network status")
            continue
        if r.status_code == 200:
            try:
                fetch_feeds.append(feedparser.parse(r.content)["entries"])
            except Exception as ret_exception:
                logging.error("Cannot parse feed from [" + url + "]. Error message: " + str(ret_exception))
        else:
            logging.warning("Got error code [" + str(r.status_code) + "] while retrieving content at [" + url + "]")
    feeds_list = [item for feed in fetch_feeds for item in feed]
    # Prepare list of news
    news_list: list[NewsFromFeed] = []
    # Scan each feed and convert it to a class element. Store the checksum to avoid dupes
    for single_feed in feeds_list:
        logging.debug("Processing [" + single_feed["link"] + "]")
        # Old RSS format
        if single_feed["summary"]:
            feed_content = remove_html(single_feed["summary"])
            # Check if valid content
            if len(feed_content) <= 10:
                logging.warning("Skipping [" + single_feed["link"] + "], empty content")   
                continue
            # Generate new article
            try:
                if single_feed["author"]:
                    new_article = NewsFromFeed(single_feed["title"], single_feed["published"], single_feed["author"], feed_content, single_feed["link"])
                else:
                    new_article = NewsFromFeed(single_feed["title"], single_feed["published"], extract_domain(single_feed["link"]), feed_content, single_feed["link"])
                news_list.append(new_article)
            except Exception as ret_exception:
                logging.warning("Cannot process [" + single_feed["link"] + "], exception: " + str(ret_exception))
        # New RSS format
        elif single_feed["description"]:
            # Check if valid content
            feed_content = remove_html(single_feed["description"])
            # Check if valid content
            if len(feed_content) <= 10:
                logging.warning("Skipping [" + single_feed["link"] + "], empty content")   
                continue
            # Generate new article
            try:
                if single_feed["dc:creator"]:
                    new_article = NewsFromFeed(single_feed["title"], single_feed["pubDate"], single_feed["dc:creator"], feed_content, single_feed["link"])
                else:
                    new_article = NewsFromFeed(single_feed["title"], single_feed["pubDate"], extract_domain(single_feed["link"]), feed_content, single_feed["link"])
            except Exception as ret_exception:
                logging.warning("Cannot process [" + single_feed["link"] + "], exception: " + str(ret_exception))
            news_list.append(new_article)
        else:
            # Unknown format
            logging.warning("Skipping [" + single_feed["link"] + "], incompatible RSS format")
            continue
    # Return list
    logging.info("Fetch [" + str(len(news_list)) + "] news")
    news_list.sort(key=lambda news: news.date, reverse=True)
    return news_list

# Handle translation
def translate_text(input_text: str, dest_lang: str = "it") -> str:
    """Translate text using Google APIs"""
    # Check if skip translations
    if noAi:
        return input_text
    # Start text rework
    logging.debug("Translating: [" + input_text + "]")
    translator = Translator()
    translator_response = None
    try:
        translator_response = translator.translate(input_text, dest=dest_lang)
    except Exception as ret_exc:
        logging.error(str(ret_exc))
        return input_text
    logging.debug(translator_response)
    if translator_response is None:
        logging.error("Unable to translate text")
        return input_text
    elif len(translator_response.text) < 10:
        logging.error("Translation was too short")
        return input_text
    return translator_response.text
    
# Database preparation
def prepare_db() -> None:
    """Prepare the sqlite store"""
    # Connect to SQLite
    logging.debug("Opening SQLite store")
    sqliteConn = get_sql_connector()
    sqliteCursor = sqliteConn.cursor()
    # Create news table
    try:
        sqliteCursor.execute("CREATE TABLE news(date, checksum)")
        logging.info("News table was generated successfully")
    except:
        logging.debug("News table already exists")
    # Count sent articles
    try:
        data_from_db = sqliteCursor.execute("SELECT checksum FROM news WHERE 1").fetchall()
        logging.info("News table contains [" + str(len(data_from_db)) + "] records")
    except Exception as returned_exception:
        logging.critical("Error while getting count of news records: " + str(returned_exception))
        raise Exception(returned_exception)
    # Create feeds table
    try:
        sqliteCursor.execute("CREATE TABLE feeds(url)")
        logging.info("Feeds table was generated successfully")
    except:
        logging.debug("Feeds table already exists")
    # Get feeds from DB
    data_from_db = sqliteCursor.execute("SELECT url FROM feeds WHERE 1").fetchall()
    if (len(data_from_db) < 1):
        logging.info("News table is empty, adding default")
        try:
            for single_url in default_urls:
                logging.debug("Adding [" + single_url + "]")
                sqliteCursor.execute("INSERT INTO feeds(url) VALUES(?)", [single_url])
            sqliteConn.commit()
            if (len(sqliteCursor.execute("SELECT url FROM feeds WHERE 1").fetchall()) < 1):
                raise Exception("Records were not added!")
            logging.debug("Default records were added")
        except Exception as returned_exception:
            logging.error(returned_exception)
            return
    else:
        logging.info("Feeds table contains [" + str(len(data_from_db)) + "] records")
    # Close DB connection
    sqliteConn.close()

# Get SQL Connector
def get_sql_connector() -> sqlite3.Connection:
    """Connect to sqlite"""
    return sqlite3.connect("store/frlbot.db", timeout=3)

# Delete old SQLite records
def remove_old_news(max_days: int = -1) -> int:
    """Delete all old feeds from the database"""
    if max_days == -1:
        max_days = get_max_news_days_from_env()
    try:
        # Get SQL cursor
        sqlCon = get_sql_connector()
        oldNews = sqlCon.cursor().execute("SELECT date FROM news WHERE date <= date('now', '-" + str(max_days) + " day')").fetchall()
        logging.info("Removing [" + str(len(oldNews)) + "] old news from DB")
        sqlCon.cursor().execute("DELETE FROM news WHERE date <= date('now', '-" + str(max_days) + " day')")
        sqlCon.commit()
        sqlCon.close()
        return len(oldNews)
    except Exception as returned_exception:
        logging.error("Cannot delete older news. " + str(returned_exception))
        return -1

# Main code
def main():
    """Main robot code"""
    logging.info("Starting bot")
    # Generate bot object
    global telegramBot
    # Track how many news we sent
    news_cnt: int = 0
    max_news = get_max_news_cnt_from_env()
    # Get SQL cursor
    sql_connector = get_sql_connector()
    # Clean data from DB
    feeds_from_db = [x[0] for x in sql_connector.cursor().execute("SELECT url FROM feeds WHERE 1").fetchall()]
    if feeds_from_db is None:
        logging.error("No news from DB")
        sql_connector.close()
        return
    logging.debug("Fetching [" + str(len(feeds_from_db)) + "] feeds")
    # Monitor exceptions and report in case of multiple errors
    exception_cnt = 0
    exception_message = ""
    # Get news from feed
    for single_news in parse_news(feeds_from_db):
        # Check if we already sent this message
        if sql_connector.cursor().execute("SELECT * FROM news WHERE checksum='" + single_news.checksum + "'").fetchone() is None:
            logging.info("Sending: [" + single_news.link + "]")
            # Check if article is no more than 30 days
            if datetime.now().replace(tzinfo=None) - single_news.date.replace(tzinfo=None) > timedelta(days=30):
                logging.debug("Article: [" + single_news.link + "] is older than 30 days, skipping")
            elif single_news.date.replace(tzinfo=None) > datetime.now().replace(tzinfo=None):
                logging.warning("Article: [" + single_news.link + "] is coming from the future?!")
            else:
                # Prepare message to send
                emoji_flag_it = emoji.emojize(":Italy:", language="alias")
                emoji_flag_en = emoji.emojize(":United_States:", language="alias")
                emoji_pencil = emoji.emojize(":pencil2:", language="alias")
                emoji_calendar = emoji.emojize(":spiral_calendar:", language="alias")
                emoji_link = emoji.emojize(":link:", language="alias")
                try:
                    telegram_payload = f"{emoji_flag_it} {translate_text(single_news.title, 'it')}\n" + \
                                        f"{emoji_flag_en} {translate_text(single_news.title, 'en')}\n" + \
                                        f"\n{emoji_pencil} {single_news.author}\n" + \
                                        f"{emoji_calendar} {single_news.date.strftime('%Y/%m/%d, %H:%M')}\n" + \
                                        f"\n{emoji_flag_it} {translate_text(single_news.summary, 'it')}\n" + \
                                        f"\n{emoji_flag_en} {translate_text(single_news.summary, 'en')}\n" + \
                                        f"\n{emoji_link} {single_news.link}"
                    if not dryRun:
                        telegramBot.send_message(get_target_chat_from_env(), telegram_payload, parse_mode="MARKDOWN")
                    else:
                        logging.info(telegram_payload)
                    if not dryRun:
                        # Store this article to DB
                        logging.debug("Adding [" + single_news.checksum + "] to store")
                        sql_connector.cursor().execute("INSERT INTO news(date, checksum) VALUES(?, ?)", [single_news.date, single_news.checksum])
                        sql_connector.commit()
                    news_cnt += 1
                except Exception as returned_exception:
                    logging.error(str(returned_exception))
                    exception_cnt += 1
                    exception_message = str(returned_exception)
        # This message was already posted
        else:
            logging.debug("Post at [" + single_news.link + "] was already sent")
        # Check errors count
        if exception_cnt > 3:
            logging.error("Too many errors, skipping this upgrade")
            if not dryRun:
                telegramBot.send_message(get_admin_chat_from_env(), "Too many errors, skipping this execution. Last error: `" + exception_message + "`")
            break
        # Stop execution after sending x elements
        if news_cnt >= max_news:
            break
    logging.debug("No more articles to process, waiting for next execution")
    # Close DB connection
    sql_connector.close()

# Check if force send
def check_arguments(argv) -> list[bool, bool, bool]:
    """Check CLI arguments"""
    try:
        opts, args = getopt.getopt(argv,"fdn",["force", "dry", "notr"])
        dry_run = False
        force_run = False
        no_translate = False
        for opt, arg in opts:
            if opt in ("-d", "--dry"):
                dry_run = True
            if opt in ("-f", "--force"):
                force_run = True
            if opt in ("-n", "--notr"):
                no_translate = True
        logging.info("DryRun: " + str(dry_run) + " - ForceRun: " + str(force_run) + " - NoAI: " + str(no_translate))
        return dry_run, force_run, no_translate
    except:
        return None

# Check if valid XML
def valid_xml(inputUrl: str) -> bool:
    """Check if XML has valid syntax"""
    try:
        getRes = requests.get(inputUrl)
        xml.dom.minidom.parseString(getRes.content)
        return True
    except:
        return False

# Cleanup old news
schedule.every().day.at("01:00").do(remove_old_news, )
# Execute bot news
schedule.every(get_post_interval_from_env()).minutes.do(main, )

def scheduler_loop():
    """Thread to handle the scheduler"""
    logging.info("Starting scheduler loop")
    while True:
        schedule.run_pending()
        time.sleep(5)

def telegram_loop():
    """Thread to handle Telegram commands"""
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
        dryRun, forceRun, noAi = check_arguments(sys.argv[1:])
    except:
        logging.critical("Invalid command line arguments have been set")
        exit()
    # Initialize Bot
    if not dryRun:
        init_bot()
        # Handle LIST command
        @telegramBot.message_handler(content_types=["text"], commands=['urllist'])
        def HandleUrlListMessage(inputMessage: telebot.types.Message):
            if inputMessage.from_user.id == get_admin_chat_from_env():
                logging.debug("URL list requested from [" + str(inputMessage.from_user.id) + "]")
                global telegramBot
                sqlCon = get_sql_connector()
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
            if inputMessage.from_user.id == get_admin_chat_from_env():
                global telegramBot
                sqlCon = get_sql_connector()
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
                        if valid_xml(splitText[1]):
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
            if inputMessage.from_user.id == get_admin_chat_from_env():
                global telegramBot
                sqlCon = get_sql_connector()
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
            if inputMessage.from_user.id == get_admin_chat_from_env():
                logging.debug("Manual bot execution requested from [" + str(inputMessage.from_user.id) + "]")
                global telegramBot
                telegramBot.reply_to(inputMessage, "Forcing bot execution")
                main()
            else:
                logging.debug("Ignoring [" + inputMessage.text + "] message from [" + str(inputMessage.from_user.id) + "]")
        # Remove old news
        @telegramBot.message_handler(content_types=["text"], commands=['rmoldnews'])
        def HandleOldNewsDelete(inputMessage: telebot.types.Message):
            if inputMessage.from_user.id == get_admin_chat_from_env():
                logging.debug("Manual news deletion requested from [" + str(inputMessage.from_user.id) + "]")
                global telegramBot
                splitMessage = inputMessage.text.split(" ")
                if len(splitMessage) != 2:
                    telegramBot.reply_to(inputMessage, "Expecting only one argument")
                elif splitMessage[1].isdigit():
                    deletedNews = remove_old_news(int(splitMessage[1]))
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
            if inputMessage.from_user.id == get_admin_chat_from_env():
                logging.debug("Adding news from CSV list")
                global telegramBot
                sqlCon = get_sql_connector()
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
                            if valid_xml(singleUrl):
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
            if inputMessage.from_user.id == get_admin_chat_from_env():
                logging.debug("Peforming news cleanup")
                global telegramBot
                telegramBot.reply_to(inputMessage, "Performing cleanup, please be patient...")
                sqlCon = get_sql_connector()
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
                        if not valid_xml(singleElement[1]):
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
            if inputMessage.from_user.id == get_admin_chat_from_env():
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
    prepare_db()
    if forceRun:
        logging.info("Starting forced execution")
        main()
        sys.exit(0)
    # Start async execution
    logging.info("Starting main loop")
    if not dryRun:
        telegramThread = threading.Thread(target=telegram_loop, name="TelegramLoop")
        telegramThread.start()
        scheduler_loop()
    else:
        main()