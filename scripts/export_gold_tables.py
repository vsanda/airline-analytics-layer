import pandas as pd
import psycopg2
import os

# Create export folders
os.makedirs("gold/csv", exist_ok=True)
os.makedirs("gold/html", exist_ok=True)
os.makedirs("gold/excel", exist_ok=True)

# Connect using environment variables
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)

def get_gold_views():
    query = """
        SELECT table_name
        FROM information_schema.views
        WHERE table_schema = 'dbt_gold'
        ORDER BY table_name;
    """
    df = pd.read_sql(query, conn)
    return df['table_name'].tolist()

def export_table(table_name):
    try:
        query = f'SELECT * FROM dbt_gold."{table_name}"'  # quotes in case of camelCase or special chars
        df = pd.read_sql(query, conn)
        df.to_csv(f"gold/csv/{table_name}.csv", index=False)
        df.to_html(f"gold/html/{table_name}.html", index=False)
        df.to_excel(f"gold/excel/{table_name}.xlsx", index=False)
        print(f"Exported {table_name}")
    except Exception as e:
        print(f"Failed to export {table_name}: {e}")

# Automatically find all dbt_gold views
gold_views = get_gold_views()

# Export all
for view in gold_views:
    export_table(view)

conn.close()
