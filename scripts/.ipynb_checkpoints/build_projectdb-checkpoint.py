import psycopg2 as psql
import os
import sys
from datetime import datetime

def main():
    password_file = os.path.join("secrets", ".psql.pass")
    if not os.path.exists(password_file):
        print(f"Error: Password file not found at {password_file}")
        sys.exit(1)

    with open(password_file, "r") as file:
        password = file.read().rstrip()

    conn_string = f"host=hadoop-04.uni.innopolis.ru port=5432 user=team18 dbname=team18_projectdb password={password}"

    try:
        # Connect to PostgreSQL
        with psql.connect(conn_string) as conn:
            print(f"[{datetime.now()}] Connected to PostgreSQL database")

            # Create tables
            print(f"[{datetime.now()}] Creating tables...")
            with open(os.path.join("sql", "create_tables.sql")) as file:
                cur = conn.cursor()
                cur.execute(file.read())
                conn.commit()
                print(f"[{datetime.now()}] Tables created successfully")

            # Import data from CSV files (takes approximately 15 minutes)
            print(f"[{datetime.now()}] Importing data from CSV files...")
            csv_files = ['2019-Oct.csv', '2019-Nov.csv', '2019-Dec.csv']

            with open(os.path.join("sql", "import_data.sql")) as file:
                import_command = file.read()

                for csv_file in csv_files:
                    csv_path = os.path.join("data", csv_file)
                    if os.path.exists(csv_path):
                        print(f"[{datetime.now()}] Importing {csv_file}...")
                        cur = conn.cursor()
                        with open(csv_path, "r") as data_file:
                            # Skip header
                            next(data_file)
                            cur.copy_expert(import_command, data_file)
                        conn.commit()
                        print(f"[{datetime.now()}] {csv_file} imported successfully")
                    else:
                        print(f"Warning: {csv_file} not found in data directory")
                        
            print(f"[{datetime.now()}] Testing database...")
            with open(os.path.join("sql", "test_database.sql")) as file:
                cur = conn.cursor()
                commands = file.read().split(';')
                for command in commands:
                    if command.strip():
                        cur.execute(command)
                        results = cur.fetchall()
                        for row in results:
                            print(row)

            print(f"[{datetime.now()}] Database build completed successfully!")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()