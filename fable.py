import requests
import json
import sqlite3

def create_tables():
    cur.execute('''
    CREATE TABLE IF NOT EXISTS book (
        id TEXT PRIMARY KEY,
        title TEXT,
        subtitle TEXT,
        cover_image TEXT,
        cover_image_small TEXT,
        published_date TEXT,
        embargoed_until TEXT,
        imprint TEXT,
        isbn TEXT,
        source TEXT,
        description TEXT,
        price_usd REAL,
        page_count INTEGER,
        chapter_count INTEGER,
        fable_summary TEXT,
        fable_prompts_document TEXT,
        url TEXT,
        audiobook TEXT,
        type TEXT,
        is_out_of_catalog BOOLEAN,
        iap_identifier TEXT,
        non_fiction BOOLEAN,
        finished_reading_at TEXT,
        finished_reading_date_type TEXT
    )
    ''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS author (
        name TEXT,
        biography TEXT,
        slug TEXT,
        book_id TEXT,
        FOREIGN KEY(book_id) REFERENCES book(id)
    )
    ''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS subject (
        category TEXT,
        book_id TEXT,
        FOREIGN KEY(book_id) REFERENCES book(id)
    )
    ''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS genre (
        id TEXT,
        name TEXT,
        type TEXT,
        book_id TEXT,
        FOREIGN KEY(book_id) REFERENCES book(id)
    )
    ''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS storygraph_tag (
        moods TEXT,
        genres TEXT,
        content_warnings TEXT,
        book_id TEXT,
        FOREIGN KEY(book_id) REFERENCES book(id)
    )
    ''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS review_summary (
        liked TEXT,
        disliked TEXT,
        disagreed TEXT,
        book_id TEXT,
        FOREIGN KEY(book_id) REFERENCES book(id)
    )
    ''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS users (
        source TEXT,
        favorite TEXT,
        sort_value TEXT,
        book_id TEXT,
        FOREIGN KEY(book_id) REFERENCES book(id)
    )
    ''')

    con.commit()

# Insert book data into the respective tables
def insert_book_data(book):
    finished_reading_date_type = ""
    finished_reading_at = ""
    try:
        finished_reading_date_type = book["finished_reading_date_type"]
        finished_reading_at = book["finished_reading_at"]
    except KeyError:
        pass
    cur.execute('''
    INSERT INTO book (id, title, subtitle, cover_image, cover_image_small, published_date, embargoed_until, imprint, isbn, source, description, price_usd, page_count, chapter_count, fable_summary, fable_prompts_document, url, audiobook, type, is_out_of_catalog, iap_identifier, non_fiction, finished_reading_at, finished_reading_date_type)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        book["id"], book["title"], book["subtitle"], book["cover_image"], book["cover_image_small"], book["published_date"], book["embargoed_until"], book["imprint"], book["isbn"], book["source"], book["description"], book["price_usd"], book["page_count"], book["chapter_count"], book["fable_summary"], book["fable_prompts_document"], book["url"], book["audiobook"], book["type"], book["is_out_of_catalog"], book["iap_identifier"], book["non_fiction"], finished_reading_at, finished_reading_date_type
    ))

    for author in book["authors"]:
        cur.execute('''
        INSERT INTO author (name, biography, slug, book_id)
        VALUES (?, ?, ?, ?)
        ''', (author["name"], author["biography"], author["slug"], book["id"]))

    for subject in book["subjects"]:
        for subsubject in subject:
            cur.execute('''
            INSERT INTO subject (category, book_id)
            VALUES (?, ?)
            ''', (subsubject, book["id"]))

    for genre in book["genres"]:
        cur.execute('''
        INSERT INTO genre (id, name, type, book_id)
        VALUES (?, ?, ?, ?)
        ''', (genre["id"], genre["name"], genre["type"], book["id"]))

    if(book["storygraph_tags"] is not None):
        cur.execute('''
        INSERT INTO storygraph_tag (moods, genres, content_warnings, book_id)
        VALUES (?, ?, ?, ?)
        ''', (json.dumps(book["storygraph_tags"]["moods"]), json.dumps(book["storygraph_tags"]["genres"]), json.dumps(book["storygraph_tags"]["content_warnings"]), book["id"]))
    else:
        cur.execute('''
        INSERT INTO storygraph_tag (moods, genres, content_warnings, book_id)
        VALUES (?, ?, ?, ?)
        ''', ("", "", "", book["id"]))

    if(book["review_summary"] is not None):
        cur.execute('''
        INSERT INTO review_summary (liked, disliked, disagreed, book_id)
        VALUES (?, ?, ?, ?)
        ''', (book["review_summary"]["liked"], book["review_summary"]["disliked"], book["review_summary"]["disagreed"], book["id"]))
    else:
        cur.execute('''
        INSERT INTO review_summary (liked, disliked, disagreed, book_id)
        VALUES (?, ?, ?, ?)
        ''', ("", "", "", book["id"]))


def main():
    create_tables()

    # url of the api of the finish book for the user
    url = ""
    canNext = True

    while canNext:
        api = requests.get(url)
        data = api.json()
        for book in data["results"]:
            insert_book_data(book["book"])
            cur.execute('''INSERT INTO users (source, favorite, sort_value, book_id) VALUES (?, ?, ?, ?)''', (book["source"], book["favorite"], book["sort_value"], book["book"]["id"]))
            con.commit()
        if data["next"] is not None:
            url = data["next"]
        else:
            canNext = False
    
if __name__ == '__main__':
    con = sqlite3.connect("db/book.db")
    cur = con.cursor()
    main()
    print("done")
    con.close()