import subprocess # control i/o
import time

def wait_for_postgres(host, max_retires=5, delay=5):
    retires = 0
    while retires < max_retires:
        try:
            result =subprocess.run( ["pg_isready", "-h", host], check=True, capture_output=True, text=True)
            if "accepting connections" in result.stdout:
                print(f"PostgreSQL is ready on {host}")
                return True
        except subprocess.CalledProcessError as e:
            print(f"PostgreSQL is not ready yet: {e}")
            retires += 1
            print(f"Retrying in {delay} seconds... ({retires}/{max_retires})")
            time.sleep(delay)
    print("Max retries reached. PostgreSQL is not ready.")
    return False

if not wait_for_postgres(host="source_postgres"):
    exit(1)

print("Starting ETL process...")

source_config = {
    'dbname': 'source_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'source_postgres'
}

destination_config = {
    'dbname': 'destination_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'destination_postgres'
}   