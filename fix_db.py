import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
postgres_url = os.environ.get("POSTGRES_URL")

if not postgres_url:
    print("Không tìm thấy POSTGRES_URL")
    exit(1)

with open("schema.sql", "r", encoding="utf-8") as f:
    sql = f.read()

try:
    conn = psycopg2.connect(postgres_url)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(sql)
    print("Applied schema.sql successfully!")
    conn.close()
except Exception as e:
    print(f"Error: {e}")
