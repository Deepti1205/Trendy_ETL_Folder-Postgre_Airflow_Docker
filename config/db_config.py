import os

DB = {
    "host": os.getenv("DB_HOST", "etl-db"),
    "user": os.getenv("DB_USER", "etl"),
    "password": os.getenv("DB_PASSWORD", "etl"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "etl_db")
}
