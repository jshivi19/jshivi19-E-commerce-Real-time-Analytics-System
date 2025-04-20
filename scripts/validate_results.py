"""
Script to validate the accuracy of results from streaming and batch processing
"""
import psycopg2
from config.settings import POSTGRES_CONFIG

def fetch_batch_results():
    """Fetch results from batch processing"""
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
            """)
            return cur.fetchall()
    except Exception as e:
        print(f"Error fetching batch results: {e}")
    finally:
        conn.close()

def fetch_streaming_results():
    """Simulate fetching results from streaming processing"""
    # This function should fetch results from the streaming output
    # For now, it returns a static example for validation
    return [("example", 10), ("hashtag", 8), ("test", 5)]

def validate_results():
    """Compare batch and streaming results for consistency"""
    batch_results = fetch_batch_results()
    streaming_results = fetch_streaming_results()

    print("Validating results...")
    print("Batch Results:", batch_results)
    print("Streaming Results:", streaming_results)

    # Compare results
    if batch_results == streaming_results:
        print("Results are consistent between batch and streaming modes.")
    else:
        print("Discrepancies found between batch and streaming results.")

if __name__ == "__main__":
    validate_results()
