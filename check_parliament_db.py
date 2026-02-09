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

cur.execute("SELECT COUNT(*) FROM parliament_notices WHERE list IS NOT true")
total_notices = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM parliament_notices WHERE list IS true")
total_list_rows = cur.fetchone()[0]

print("PARLIAMENT NOTICES DATABASE SUMMARY")
print(f"Total notice rows: {total_notices}")
print(f"Total list rows: {total_list_rows}")
print(f"Grand total: {total_notices + total_list_rows}")
print()
print("Showing latest 5 notices:")
print()

cur.execute("SELECT id, url, uri, data FROM parliament_notices WHERE list IS NOT true ORDER BY inserted_at DESC LIMIT 5")
rows = cur.fetchall()

for row in rows:
    data = row[3] if row[3] else {}
    print(f"ID: {row[0][:16]}...")
    print(f"URL: {row[1]}")
    print(f"Title: {data.get('title', 'N/A')[:80]}")
    print(f"BS Date: {data.get('bs_date', 'N/A')}")
    print()

conn.close()
