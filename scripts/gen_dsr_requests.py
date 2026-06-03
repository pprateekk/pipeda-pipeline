import uuid
import random
from datetime import datetime, timedelta, timezone
import psycopg2
from faker import Faker
from dotenv import load_dotenv
import os

load_dotenv()
fake = Faker()

REQUEST_TYPES = ['access', 'correction', 'withdraw']
STATUSES = ['open', 'in_progress', 'resolved', 'rejected']

def generate_dsr_requests(n=500):
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'), port=os.getenv('DB_PORT'),
        dbname=os.getenv('DB_NAME'), user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    user_ids = get_existing_user_ids(conn)  #pull real users
    conn.close()

    # user_ids = [uuid.uuid4() for _ in range(500)]
    requests = []
    now = datetime.now(tz=timezone.utc)

    for i in range(n):

        # inject SLA breaches: 20 open requests older than 30 days
        if i < 20:
            submitted = now - timedelta(days=random.randint(31, 90))
            status = random.choice(['open', 'in_progress'])
            resolved_at = None

        else:
            status = random.choice(STATUSES)

            if status in ('open', 'in_progress'):
                # Keep these recent so they don't look like breaches
                submitted = now - timedelta(days=random.randint(1, 29))
                resolved_at = None
            else:
                # resolved/rejected can be anywhere in the historical range
                submitted = fake.date_time_between_dates(
                    datetime(2023, 6, 1, tzinfo=timezone.utc),
                    datetime(2024, 11, 30, tzinfo=timezone.utc),
                    tzinfo=timezone.utc
                )
                resolved_at = submitted + timedelta(days=random.randint(1, 25)) if status == 'resolved' else None

        requests.append({
            'user_id': random.choice(user_ids),
            'request_type': random.choice(REQUEST_TYPES),
            'submitted_at': submitted,
            'resolved_at': resolved_at,
            'status': status
        })
    return requests

def load_requests(requests):
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'), port=os.getenv('DB_PORT'),
        dbname=os.getenv('DB_NAME'), user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    cur = conn.cursor()
    for r in requests:
        cur.execute("""
            INSERT INTO raw.dsr_requests
                (user_id, request_type, submitted_at, resolved_at, status)
            VALUES (%s,%s,%s,%s,%s)
        """, (str(r['user_id']), r['request_type'], r['submitted_at'], r['resolved_at'], r['status']))
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded{len(requests)} DSR requests, including 20 deliberate SLA breaches")

def get_existing_user_ids(conn):
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT user_id FROM raw.consent_events")
    rows = cur.fetchall()
    cur.close()
    return [row[0] for row in rows]

if __name__ == '__main__':
    requests = generate_dsr_requests(500)
    load_requests(requests)
