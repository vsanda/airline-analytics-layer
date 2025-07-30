import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load environment variables
load_dotenv()

# Create export folders
os.makedirs("silver/csv", exist_ok=True)
os.makedirs("silver/html", exist_ok=True)
os.makedirs("silver/excel", exist_ok=True)

# Connect using environment variables
engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

def get_silver_views():
    query = """
        SELECT table_name
        FROM information_schema.views
        WHERE table_schema = 'dbt_silver'
        ORDER BY table_name;
    """
    df = pd.read_sql(query, engine)
    return df['table_name'].tolist()

def export_table(table_name):
    try:
        query = f'SELECT * FROM dbt_silver."{table_name}" LIMIT 25'
        df = pd.read_sql(query, engine)

        # Strip timezones for Excel support
        for col in df.select_dtypes(include=["datetimetz"]).columns:
                df[col] = df[col].dt.tz_convert(None)

        df.to_csv(f"silver/csv/{table_name}.csv", index=False)
        df.to_html(f"silver/html/{table_name}.html", index=False)
        df.to_excel(f"silver/excel/{table_name}.xlsx", index=False)
        print(f"Exported {table_name} (25 rows)")
    except Exception as e:
        print(f"Failed to export {table_name}: {e}")

# Automatically find all dbt_silver views
silver_views = get_silver_views()

# Export all
for view in silver_views:
    export_table(view)

