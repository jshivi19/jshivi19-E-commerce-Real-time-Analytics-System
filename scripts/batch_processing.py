"""
Script to perform batch processing on stored tweet data
"""
import psycopg2
from config.settings import POSTGRES_CONFIG

def get_top_hashtags(limit=10):
    """Query the database to get the top hashtags by count"""
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT word, COUNT(*) as count
                FROM (
                    SELECT jsonb_array_elements_text(hashtags) as word
                    FROM tweets
                ) subquery
                GROUP BY word
                ORDER BY count DESC
                LIMIT %s
            """, (limit,))
            results = cur.fetchall()
            print("Top Hashtags:")
            for row in results:
                print(f"{row[0]}: {row[1]}")
    except Exception as e:
        print(f"Error querying top hashtags: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    print("Performing batch processing...")
    get_top_hashtags()
