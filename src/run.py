import os
from dotenv import load_dotenv

load_dotenv()

import asyncio
from nthrow.utils import create_db_connection, create_store, uri_clean, uri_row_count
from extractor import QuoteExtractor

TABLE_NAME = "quotes"

DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}


async def main():
    conn = create_db_connection(**DB_CONFIG)
    create_store(conn, TABLE_NAME)
    
    extractor = QuoteExtractor(conn, TABLE_NAME)
    extractor.set_list_info("https://quotes.toscrape.com/")
    
    # clear old data before starting
    uri_clean(extractor.uri, conn, TABLE_NAME)
    
    async with await extractor.create_session() as session:
        extractor.session = session
        
        # grab first page
        await extractor.collect_rows(extractor.get_list_row())
        
        # keep going until we hit the end
        while extractor.should_run_again():
            row = extractor.get_list_row()
            next_page = row.get("state", {}).get("pagination", {}).get("to")
            
            if next_page is None:
                break
                
            print(f"Fetching page {next_page}...")
            extractor._reset_run_times()
            await extractor.collect_rows(row)
    
    count = uri_row_count(extractor.uri, conn, TABLE_NAME, partial=False)
    print(f"\nDone! Got {count} quotes total")
    
    conn.close()


if __name__ == "__main__":
    asyncio.run(main())
