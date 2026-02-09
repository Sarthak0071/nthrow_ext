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
    """
    Extract all pages from quotes.toscrape.com.
    Matches the nthrow test pattern: multiple collect_rows() calls in one execution.
    """
    conn = create_db_connection(**DB_CONFIG)
    create_store(conn, TABLE_NAME)
    
    extractor = QuoteExtractor(conn, TABLE_NAME)
    extractor.set_list_info("https://quotes.toscrape.com/")
    
    # Clean old data - matches test pattern (line 37)
    uri_clean(extractor.uri, conn, TABLE_NAME)
    
    async with await extractor.create_session() as session:
        extractor.session = session
        
        # First page
        await extractor.collect_rows(extractor.get_list_row())
        
        # Continue while pagination exists
        # This matches what the tests do - multiple collect_rows() calls
        while extractor.should_run_again():
            row = extractor.get_list_row()
            next_page = row.get("state", {}).get("pagination", {}).get("to")
            
            if next_page is None:
                break
                
            print(f"Fetching page {next_page}...")
            extractor._reset_run_times()  # Reset like the test does (line 84)
            await extractor.collect_rows(row)
    
    count = uri_row_count(extractor.uri, conn, TABLE_NAME, partial=False)
    print(f"\n✓ Extraction complete! Total quotes: {count}")
    
    conn.close()


if __name__ == "__main__":
    asyncio.run(main())
