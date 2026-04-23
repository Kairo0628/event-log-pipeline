import psycopg2
import os
import io, csv
from user_agents import parse
from datetime import datetime


def get_conn():
    """
    postgreSQL 데이터베이스와 연결합니다.
    """
    conn = psycopg2.connect(
        host = os.getenv('DB_HOST', 'postgres'),
        port = os.getenv('DB_PORT', 5432),
        dbname = os.getenv('DB_NAME', 'events'),
        user = os.getenv('DB_USER', 'admin'),
        password = os.getenv('DB_PASSWORD', 'admin')
    )

    return conn

def create_table_not_exist():
    """
    DB에 데이터를 넣기 전, 테이블을 먼저 생성합니다.
    테이블이 존재한다면 동작하지 않습니다.
    """
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
    """
    유저 에이전트를 파싱하여 기기, 운영체제, 브라우저 정보를 추가합니다.
    
    전체 데이터를 CSV로 변환하여 COPY 명령어를 통해 적재합니다.
    Bulk Insert 방식이므로 execute, executemany 등의 함수보다 빠릅니다.
    """
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
