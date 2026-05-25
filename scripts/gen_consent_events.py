import uuid
import random
from datetime import datetime, timedelta, timezone
from faker import Faker
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()
fake = Faker()

EVENT_TYPES = ['consent_granted', 'consent_revoked', 'consent_updated']
PURPOSES = ['marketing', 'analytics', 'third_party_sharing', 'profiling']
CHANNELS = ['web', 'mobile', 'email', 'in_app']
POLICY_VERSIONS = ['v1.0', 'v1.1', 'v2.0', 'v2.1', 'v2.2']
POLICY_DATES = {
    'v1.0': datetime(2023, 1, 1, tzinfo=timezone.utc),
    'v1.1': datetime(2023, 4, 15, tzinfo=timezone.utc),
    'v2.0': datetime(2023, 9, 1, tzinfo=timezone.utc),
    'v2.1': datetime(2024, 2, 10, tzinfo=timezone.utc),
    'v2.2': datetime(2024, 8, 1, tzinfo=timezone.utc),
}

#return the correct policy version based on the date
def get_policy_version_for_date(event_date):
    applicable = [v for v, d in POLICY_DATES.items() if d <= event_date]
    return applicable[-1] if applicable else 'v1.0'

def generate_events(n=10000):
    user_ids = [uuid.uuid4() for _ in range(500)]  #500 distinct users
    start_date = datetime(2023, 1, 1, tzinfo=timezone.utc)
    end_date = datetime(2024, 12, 31, tzinfo=timezone.utc)

    events = []
    for _ in range(n):
        ts = fake.date_time_between_dates(start_date, end_date, tzinfo=timezone.utc)
        policy = get_policy_version_for_date(ts)
        events.append({
            'user_id': random.choice(user_ids),
            'event_type': random.choice(EVENT_TYPES),
            'purpose': random.choice(PURPOSES),
            'channel': random.choice(CHANNELS),
            'policy_version_id': policy,
            'event_timestamp': ts,
            'metadata': {'ip_country': fake.country_code(), 'device': random.choice(['desktop', 'mobile', 'tablet'])}
        })
    return events, user_ids

def load_events(events):
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'), port=os.getenv('DB_PORT'),
        dbname=os.getenv('DB_NAME'), user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    cur = conn.cursor()
    import json
    for e in events:
        cur.execute("""
            INSERT INTO raw.consent_events
                (user_id, event_type, purpose, channel, policy_version_id, event_timestamp, metadata)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (str(e['user_id']), e['event_type'], e['purpose'], e['channel'],
              e['policy_version_id'], e['event_timestamp'], json.dumps(e['metadata'])))
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {len(events)} consent events.")

if __name__ == '__main__':
    events, user_ids = generate_events(10000)
    load_events(events)
