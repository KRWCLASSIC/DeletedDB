import discord
import sqlite3
import os
import random
import string
import aiohttp
import hashlib
import csv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import asyncio

intents = discord.Intents.default()
intents.message_content = True
intents.messages = True
client = discord.Client(intents=intents)

# Database folder path
db_folder = 'db'

# Ensure the db folder exists
if not os.path.exists(db_folder):
    os.makedirs(db_folder)

# Database paths
deleted_db_path = os.path.join(db_folder, 'deleted_db.sqlite')
edited_db_path = os.path.join(db_folder, 'edited_db.sqlite')
cache_db_path = os.path.join(db_folder, 'cache_db.sqlite')

# Create the deleted_db.sqlite database and tables
def setup_deleted_db():
    conn = sqlite3.connect(deleted_db_path, check_same_thread=False)
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS servers (
        id TEXT PRIMARY KEY,
        name TEXT
    )''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        username TEXT,
        discriminator TEXT,
        profile_pic TEXT
    )''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS deleted_messages (
        entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
        id TEXT,
        content TEXT,
        author_id TEXT,
        server_id TEXT,
        channel_id TEXT,
        timestamp TEXT,
        attachments TEXT,
        FOREIGN KEY (author_id) REFERENCES users (id),
        FOREIGN KEY (server_id) REFERENCES servers (id)
    )''')

    conn.commit()
    return conn, cursor

# Create the edited_db.sqlite database and tables
def setup_edited_db():
    conn = sqlite3.connect(edited_db_path, check_same_thread=False)
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS servers (
        id TEXT PRIMARY KEY,
        name TEXT
    )''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        username TEXT,
        discriminator TEXT,
        profile_pic TEXT
    )''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS edited_messages (
        entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
        id TEXT,
        old_content TEXT,
        new_content TEXT,
        edit_timestamp TEXT,
        author_id TEXT,
        server_id TEXT,
        channel_id TEXT,
        attachments TEXT,
        attachment_removed TEXT,
        FOREIGN KEY (author_id) REFERENCES users (id),
        FOREIGN KEY (server_id) REFERENCES servers (id)
    )''')

    conn.commit()
    return conn, cursor

# Initialize databases
deleted_conn, deleted_cursor = setup_deleted_db()
edited_conn, edited_cursor = setup_edited_db()

# Ensure the .cache folder exists before creating cache_db.sqlite
if not os.path.exists('.cache'):
    os.makedirs('.cache')

# Cache database setup
def create_cache_table():
    cache_conn = sqlite3.connect(cache_db_path, check_same_thread=False)
    cache_cursor = cache_conn.cursor()

    cache_cursor.execute('''
    CREATE TABLE IF NOT EXISTS cache (
        url TEXT,
        attachment_hash TEXT PRIMARY KEY,
        file TEXT
    )''')
    cache_conn.commit()
    return cache_conn, cache_cursor

cache_conn, cache_cursor = create_cache_table()

# Ensure the csv folder exists
if not os.path.exists('csv'):
    os.makedirs('csv')

# Helper function to generate a random filename
def generate_random_filename(extension):
    random_str = ''.join(random.choices(string.ascii_letters + string.digits, k=12))
    return f"{random_str}.{extension}.cache"

# Calculate hash for the attachment content
def hash_attachment(content):
    return hashlib.sha256(content).hexdigest()

# Initialize a ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=5)

# Function to download and cache attachments
async def async_download(url):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    content = await response.read()
                    url_hash = hash_attachment(content)

                    cache_conn = sqlite3.connect(cache_db_path, check_same_thread=False)
                    cache_cursor = cache_conn.cursor()

                    cache_cursor.execute('SELECT file FROM cache WHERE attachment_hash = ?', (url_hash,))
                    result = cache_cursor.fetchone()

                    if result:
                        cache_conn.close()
                        return result[0]

                    base_url = url.split('?')[0]
                    extension = base_url.split('.')[-1]
                    filename = generate_random_filename(extension)
                    filepath = os.path.join('.cache', filename)

                    with open(filepath, 'wb') as f:
                        f.write(content)

                    cache_cursor.execute('''
                    INSERT INTO cache (url, attachment_hash, file)
                    VALUES (?, ?, ?)
                    ''', (url, url_hash, filename))
                    cache_conn.commit()
                    cache_conn.close()

                    return filename
    except Exception as e:
        print(f"Error downloading or caching file: {e}")
    return None

async def cache_attachment(url):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, lambda: asyncio.run(async_download(url)))

# Helper functions
def save_user(user, cursor, conn):
    cursor.execute('''
    INSERT OR IGNORE INTO users (id, username, discriminator, profile_pic)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
    username = excluded.username,
    discriminator = excluded.discriminator,
    profile_pic = excluded.profile_pic
    ''', (user.id, user.name, user.discriminator, str(user.display_avatar.url)))
    conn.commit()

def save_server(server, cursor, conn):
    cursor.execute('''
    INSERT OR IGNORE INTO servers (id, name)
    VALUES (?, ?)
    ''', (server.id, server.name))
    conn.commit()

async def save_deleted_message(message):
    attachments = []
    for attachment in message.attachments:
        filename = await cache_attachment(attachment.url)
        if filename:
            attachments.append(filename)

    # Insert a new record for deleted messages
    deleted_cursor.execute('''
    INSERT INTO deleted_messages (id, content, author_id, server_id, channel_id, timestamp, attachments)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (message.id, message.content, message.author.id, message.guild.id, message.channel.id, datetime.now().isoformat(), ', '.join(attachments)))

    deleted_conn.commit()

async def handle_edit(before, after):
    # Cache old attachments
    old_attachments_tasks = [cache_attachment(att.url) for att in before.attachments]
    old_attachments = await asyncio.gather(*old_attachments_tasks)

    # Cache new attachments (only if they were removed in the edit)
    old_attachment_urls = {att.url for att in before.attachments}
    new_attachment_urls = {att.url for att in after.attachments}
    removed_attachment_urls = old_attachment_urls - new_attachment_urls
    
    removed_attachments_tasks = [cache_attachment(url) for url in removed_attachment_urls]
    removed_attachments = await asyncio.gather(*removed_attachments_tasks)

    # Save the edit
    edited_cursor.execute('''
    INSERT INTO edited_messages (id, old_content, new_content, edit_timestamp, author_id, server_id, channel_id, attachments, attachment_removed)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        before.id,
        before.content,
        after.content,
        datetime.now().isoformat(),
        before.author.id,
        before.guild.id,
        before.channel.id,
        ', '.join(old_attachments),  # Old attachments
        ', '.join(removed_attachments)  # Removed attachments
    ))
    edited_conn.commit()

@client.event
async def on_message_delete(message):
    save_user(message.author, deleted_cursor, deleted_conn)
    save_server(message.guild, deleted_cursor, deleted_conn)
    await save_deleted_message(message)

@client.event
async def on_message_edit(before, after):
    save_user(before.author, edited_cursor, edited_conn)
    save_server(before.guild, edited_cursor, edited_conn)
    await handle_edit(before, after)

@client.event
async def on_ready():
    print(f'Logged in as {client.user}')

# Command to export the database to CSV
@client.event
async def on_message(message):
    if message.content.startswith('!csv'):
        export_to_csv()
        await message.channel.send("Databases have been exported to `csv/deleted_db.csv`, `csv/edited_db.csv`, and `csv/cache_db.csv`.")

# Export both databases to CSV
def export_to_csv():
    try:
        # Export deleted_db.sqlite
        with open('csv/deleted_db.csv', 'w', newline='') as csvfile:
            fieldnames = ['entry_id', 'id', 'content', 'author_id', 'server_id', 'channel_id', 'timestamp', 'attachments']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            deleted_cursor.execute('SELECT * FROM deleted_messages')
            for row in deleted_cursor.fetchall():
                writer.writerow({
                    'entry_id': row[0],
                    'id': row[1],
                    'content': row[2],
                    'author_id': row[3],
                    'server_id': row[4],
                    'channel_id': row[5],
                    'timestamp': row[6],
                    'attachments': row[7],
                })

        print("Exported to csv/deleted_db.csv")

        # Export edited_db.sqlite
        with open('csv/edited_db.csv', 'w', newline='') as csvfile:
            fieldnames = ['entry_id', 'id', 'old_content', 'new_content', 'edit_timestamp', 'author_id', 'server_id', 'channel_id', 'attachments', 'attachment_removed']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            edited_cursor.execute('SELECT * FROM edited_messages')
            for row in edited_cursor.fetchall():
                writer.writerow({
                    'entry_id': row[0],
                    'id': row[1],
                    'old_content': row[2],
                    'new_content': row[3],
                    'edit_timestamp': row[4],
                    'author_id': row[5],
                    'server_id': row[6],
                    'channel_id': row[7],
                    'attachments': row[8],
                    'attachment_removed': row[9],
                })

        print("Exported to csv/edited_db.csv")

        # Export cache_db.sqlite
        with open('csv/cache_db.csv', 'w', newline='') as csvfile:
            fieldnames = ['url', 'attachment_hash', 'file']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            cache_cursor.execute('SELECT * FROM cache')
            for row in cache_cursor.fetchall():
                writer.writerow({
                    'url': row[0],
                    'attachment_hash': row[1],
                    'file': row[2],
                })

        print("Exported to csv/cache_db.csv")
    except Exception as e:
        print(f"Error exporting CSV files: {e}")

# Run the bot
token = open('token.txt').read().strip()
client.run(token)
