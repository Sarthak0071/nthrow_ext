import psycopg2
from dotenv import load_dotenv
import os
import json

load_dotenv()

conn = psycopg2.connect(
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)

cur = conn.cursor()

# Get total count
cur.execute("SELECT COUNT(*) FROM quotes WHERE list IS NOT true")
total_quotes = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM quotes WHERE list IS true")
total_list_rows = cur.fetchone()[0]

print(f"=" * 50)
print(f"📊 DATABASE SUMMARY")
print(f"=" * 50)
print(f"Total quote rows: {total_quotes}")
print(f"Total list rows: {total_list_rows}")
print(f"Grand total: {total_quotes + total_list_rows}")
print(f"=" * 50)
print()
print(f"Showing latest 5 quotes:\n")

cur.execute("SELECT * FROM quotes ORDER BY inserted_at DESC LIMIT 5")
rows = cur.fetchall()

for row in rows:
    print(f"ID: {row[0]}")
    print(f"URL: {row[1]}")
    print(f"URI: {row[2]}")
    print(f"List: {row[4]}")
    print(f"Partial: {row[5]}")
    if row[3]:
        print(f"State: {json.dumps(row[3], indent=2)}")
    print("-" * 50)

conn.close()
