import psycopg2
import io, csv
from user_agents import parse
from datetime import datetime

def get_conn():
    conn = psycopg2.connect(
        host = 'localhost',
        dbname = 'events',
        user = 'admin',
        password = 'admin',
    )

    return conn

def create_table_not_exist():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_event_log (
            seq SERIAL PRIMARY KEY,
            user_id INT,
            user_name TEXT,
            user_agent TEXT,
            device TEXT,
            os TEXT,
            browser TEXT,
            ip TEXT,
            url TEXT,
            session TEXT,
            event_type TEXT,
            event_timestamp TIMESTAMP
        )
    """)
    conn.commit()

    conn.close()

def insert_data(all_event):
    conn = get_conn()
    cur = conn.cursor()

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    for i in all_event:
        ua = parse(i['user_agent'])
        device = 'mobile' if ua.is_mobile else 'tablet' if ua.is_tablet else 'desktop'
        browser = ua.browser.family
        os = ua.os.family

        writer.writerow([
            i['id'],
            i['user'],
            ua,
            device,
            os,
            browser,
            i['ip'],
            i['url'],
            i['session'],
            i['event_type'],
            datetime.fromtimestamp(i['timestamp'])
        ])
    buffer.seek(0)

    cur.copy_expert("""
        COPY raw_event_log (
            user_id,
            user_name,
            user_agent,
            device,
            os,
            browser,
            ip,
            url,
            session,
            event_type,
            event_timestamp
        ) FROM STDIN WITH (FORMAT CSV)
    """, buffer)
    conn.commit()

    conn.close()
