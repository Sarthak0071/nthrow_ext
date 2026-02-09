import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg2.connect(
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)

cur = conn.cursor()

print("TAG QUOTES DATABASE CHECK")
print()

cur.execute("""
    SELECT 
        data->>'tag' as tag,
        COUNT(*) as quote_count
    FROM all_quotes
    WHERE list IS NOT TRUE
    GROUP BY data->>'tag'
    ORDER BY quote_count DESC
""")

print("Quotes by tag:")
for row in cur.fetchall():
    print(f"  {row[0]:<20} : {row[1]} quotes")

print()

cur.execute("""
    SELECT 
        data->>'tag' as tag,
        data->>'text' as quote,
        data->>'author' as author
    FROM all_quotes
    WHERE list IS NOT TRUE
    LIMIT 5
""")

print("Sample quotes:")
for row in cur.fetchall():
    print(f"  [{row[0]}] {row[1][:60]}... - {row[2]}")

print()

conn.close()
