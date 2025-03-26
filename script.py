import sys
import pandas as pd
import mysql.connector
import numpy as np
import logging
import os

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Set up the log file path inside the script's directory
log_file_path = os.path.join(script_dir, 'nifi_script_error.log')
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    # Read command-line arguments
    csv_file = sys.argv[1]
    db_url = sys.argv[2]
    db_user = sys.argv[3]
    db_password = sys.argv[4]
    db = sys.argv[5]

    # Extract table name from the file name (use os.path.basename for correct path handling)
    table_name = os.path.splitext(os.path.basename(csv_file))[0].lower()

    logging.info(f"Processing file: {csv_file}, Target Table: {table_name}")

    # Connect to MySQL
    conn = mysql.connector.connect(
        host=db_url,
        user=db_user,
        password=db_password,
        database=db
    )
    cursor = conn.cursor()

    # Read CSV file
    df = pd.read_csv(csv_file)

    # Infer column types
    def infer_sql_type(dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return "INT"
        elif pd.api.types.is_float_dtype(dtype):
            return "FLOAT"
        elif pd.api.types.is_bool_dtype(dtype):
            return "BOOLEAN"
        else:
            return "TEXT"

    # Generate CREATE TABLE statement
    columns = ", ".join([f"{col} {infer_sql_type(df[col].dtype)}" for col in df.columns])
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"

    cursor.execute(create_table_sql)
    conn.commit()

    # Insert data into table
    columns = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    for _, row in df.iterrows():
        row = [None if (isinstance(x, float) and np.isnan(x)) else x for x in row]
        cursor.execute(insert_sql, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()

    logging.info(f"Successfully processed {csv_file} and loaded data into {table_name}.")

except Exception as e:
    logging.error(f"Error occurred: {str(e)}")
    logging.exception("Exception details:")
    sys.stderr.write(f"Error occurred: {str(e)}\n")
