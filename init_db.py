import psycopg
import os

credentials = {
    "host": os.getenv("DB_HOST"),
    "dbname": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT")),
    "user": os.getenv("DB_ADMIN"),
    "password": os.getenv("DB_PW"),
}

def init_db():
    with psycopg.connect(**credentials) as conn, conn.cursor() as cur, open('schema.sql', 'r') as file:
       cur.execute(file.read())

if __name__ == "__main__":
    init_db()