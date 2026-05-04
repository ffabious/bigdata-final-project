"""Build and validate the team18 PostgreSQL project database."""

import os
import sys
from datetime import datetime

import psycopg2 as psql


CSV_FILES = ("2019-Oct.csv", "2019-Nov.csv", "2019-Dec.csv")


def log(message):
    """Print a timestamped status message."""
    print(f"[{datetime.now()}] {message}")


def read_password():
    """Read the PostgreSQL password from the secrets directory."""
    password_file = os.path.join("secrets", ".psql.pass")
    if not os.path.exists(password_file):
        print(f"Error: Password file not found at {password_file}")
        sys.exit(1)

    with open(password_file, "r", encoding="utf-8") as file:
        return file.read().rstrip()


def execute_sql_file(connection, sql_path):
    """Execute all SQL statements stored in a file."""
    with open(sql_path, "r", encoding="utf-8") as file:
        cursor = connection.cursor()
        cursor.execute(file.read())
        connection.commit()


def import_csv_files(connection, import_command):
    """Import available raw CSV files into PostgreSQL."""
    for csv_file in CSV_FILES:
        csv_path = os.path.join("data", csv_file)
        if not os.path.exists(csv_path):
            print(f"Warning: {csv_file} not found in data directory")
            continue

        log(f"Importing {csv_file}...")
        cursor = connection.cursor()
        with open(csv_path, "r", encoding="utf-8") as data_file:
            next(data_file)
            cursor.copy_expert(import_command, data_file)
        connection.commit()
        log(f"{csv_file} imported successfully")


def test_database(connection):
    """Run validation queries against the populated database."""
    with open(os.path.join("sql", "test_database.sql"), "r", encoding="utf-8") as file:
        cursor = connection.cursor()
        commands = file.read().split(";")

    for command in commands:
        if command.strip():
            cursor.execute(command)
            for row in cursor.fetchall():
                print(row)


def main():
    """Create tables, import CSV data, and run database validation queries."""
    password = read_password()
    conn_string = (
        "host=hadoop-04.uni.innopolis.ru port=5432 "
        f"user=team18 dbname=team18_projectdb password={password}"
    )

    try:
        with psql.connect(conn_string) as connection:
            log("Connected to PostgreSQL database")

            log("Creating tables...")
            execute_sql_file(connection, os.path.join("sql", "create_tables.sql"))
            log("Tables created successfully")

            log("Importing data from CSV files...")
            with open(os.path.join("sql", "import_data.sql"), "r", encoding="utf-8") as file:
                import_csv_files(connection, file.read())

            log("Testing database...")
            test_database(connection)
            log("Database build completed successfully!")

    except (OSError, psql.Error) as error:
        print(f"Error: {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
