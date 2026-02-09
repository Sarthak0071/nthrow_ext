import os
from dotenv import load_dotenv

load_dotenv()

import asyncio
from nthrow.utils import create_db_connection, create_store, uri_row_count
from tag_quotes_extractor import TagQuotesExtractor

TABLE_NAME = "all_quotes"

DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}


async def scrape_tag(tag, conn):
    extractor = TagQuotesExtractor(tag, conn, TABLE_NAME)
    extractor.set_list_info(f"https://quotes.toscrape.com/tag/{tag}/")
    
    async with await extractor.create_session() as session:
        extractor.session = session
        
        await extractor.collect_rows(extractor.get_list_row())
        
        while extractor.should_run_again():
            row = extractor.get_list_row()
            next_page = row.get("state", {}).get("pagination", {}).get("to")
            
            if next_page is None:
                break
                
            print(f"[{tag}] Fetching page {next_page}...")
            extractor._reset_run_times()
            await extractor.collect_rows(row)
    
    count = uri_row_count(extractor.uri, conn, TABLE_NAME, partial=False)
    print(f"[{tag}] Done. Got {count} quotes")


async def main():
    conn = create_db_connection(**DB_CONFIG)
    create_store(conn, TABLE_NAME)
    
    tags = ["humor", "love", "life", "inspirational"]
    
    print(f"Starting parallel scraping for {len(tags)} tags")
    print()
    
    tasks = [scrape_tag(tag, conn) for tag in tags]
    await asyncio.gather(*tasks)
    
    print()
    print("All tags completed")
    
    conn.close()


if __name__ == "__main__":
    asyncio.run(main())
