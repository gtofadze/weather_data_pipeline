def check_if_db_exists(conn, cur, db_name_to_check):

    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name_to_check,))
    exists = cur.fetchone() is not None

    return exists
