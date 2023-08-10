# RSS Feed to Telegram

This bot reads articles from a list of RSS feeds, translate them to a custom language and sends the output to a Telegram chat (can be any chat, group or channel)

## Configuration

Working variables are set via environments, these are the used ones:

- `BOT_TOKEN`: contains the Telegram API Token
- `BOT_TARGET`: contains the ID of the Telegram chat were messages should be sent (bot must be admin in case of a channel)
- `BOT_ADMIN`: contains the ID of the bot administrator, the only one which is allowed to send configuration messages (can be a group)
- `MAX_NEWS_AGE`: contains the maximum age in days for an article to be valid
- `NEWS_COUNT`: how many news should be sent per each interval
- `POST_INTERVAL`: how many minutes between publications

## Admin commands

- `/urllist`: returns the list of RSS feeds
- `/addfeed [url]`: adds a new URL to the RSS feeds list
- `/rmfeed [id]`: removes the specified ID from the RSS list
- `/force`: forces a bot execution
- `/rmoldnews`: removes old news from the DB (older than `MAX_NEWS_AGE` days)
- `/addcsv [url],[url],[...]`: adds a list of RSS feeds separated by commas
- `/dbcleanup`: check if RSS feeds are valid or duplicated
- `/sqlitebackup`: makes a backup of the SQLite database

## Functioning

Every `POST_INTERVAL` minutes, the bot fetches a list of feeds (that is stored in a SQLite file), checks if they are younger than `MAX_NEWS_AGE` days and sends it to the `BOT_TARGET`. Once the message is sent, the checksum for the article URL is calculated and stored in the DB (this is needed to avoid duplicate messages).

Administrator can add, remove, view feeds via custom commands. Database backup is also possible
