import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Create export folders
os.makedirs("bronze/csv", exist_ok=True)
os.makedirs("bronze/html", exist_ok=True)
os.makedirs("bronze/excel", exist_ok=True)

# Connect using environment variables
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)

def get_bronze_views():
    query = """
        SELECT table_name
        FROM information_schema.views
        WHERE table_schema = 'dbt_bronze'
        ORDER BY table_name;
    """
    df = pd.read_sql(query, conn)
    return df['table_name'].tolist()

def export_table(table_name):
    try:
        query = f'SELECT * FROM dbt_bronze."{table_name}" LIMIT 25;'
        df = pd.read_sql(query, conn)
        df.to_csv(f"bronze/csv/{table_name}.csv", index=False)
        df.to_html(f"bronze/html/{table_name}.html", index=False)
        df.to_excel(f"bronze/excel/{table_name}.xlsx", index=False)
        print(f"Exported {table_name} (25 rows)")
    except Exception as e:
        print(f"Failed to export {table_name}: {e}")

# Automatically find all dbt_bronze views
bronze_views = get_bronze_views()

# Export all
for view in bronze_views:
    export_table(view)

conn.close()
