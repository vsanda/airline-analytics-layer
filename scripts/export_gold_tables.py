import pandas as pd
import psycopg2
import os
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine

for folder_type in ["agg", "granular"]:
    for fmt in ["csv", "html", "excel", "markdown"]:
        os.makedirs(f"gold/{folder_type}/{fmt}", exist_ok=True)

# Connect using environment variables
engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

def get_gold_views():
    query = """
        SELECT table_name
        FROM information_schema.views
        WHERE table_schema = 'dbt_gold'
        ORDER BY table_name;
    """
    df = pd.read_sql(query, engine)
    return df['table_name'].tolist()

def export_table(table_name):
    folder_type = "agg" if "monthly" in table_name.lower() else "granular"
    base_path = f"gold/{folder_type}"

    try:
        query = f'SELECT * FROM dbt_gold."{table_name}" LIMIT 25'
        df = pd.read_sql(query, engine)

        # Strip timezones for Excel
        for col in df.select_dtypes(include=["datetimetz"]).columns:
            df[col] = df[col].dt.tz_convert(None)

        # Detect monetary columns & clean them
        money_cols = [c for c in df.columns if any(x in c.lower() for x in ["cost", "revenue", "profit"])]
        for col in money_cols:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r'[\$,]', '', regex=True)
                .replace("nan", np.nan)
                .astype(float)
            )

        # Styled HTML for monthly (agg) tables
        if folder_type == "agg":
            styled = (
                df.style
                .set_caption(f"{table_name} Report")
                .set_table_styles([
                    {'selector': 'th', 'props': [('background-color', '#f2f2f2'), ('text-align', 'center')]},
                    {'selector': 'td', 'props': [('text-align', 'center')]}
                ])
                .format({col: "${:,.2f}" for col in money_cols})
            )
            try:
                import matplotlib
                styled = styled.background_gradient(subset=[c for c in df.columns if "delay" in c], cmap='Reds')
            except ImportError:
                print(f"Skipping gradient for {table_name} (matplotlib not installed)")

            styled.to_html(f"{base_path}/html/{table_name}.html")
        else:
            df.to_html(f"{base_path}/html/{table_name}.html", index=False)

        # Export CSV, Excel, Markdown
        df.to_csv(f"{base_path}/csv/{table_name}.csv", index=False)
        df.to_excel(f"{base_path}/excel/{table_name}.xlsx", index=False)
        with open(f"{base_path}/markdown/{table_name}.md", "w") as f:
            f.write(df.to_markdown(index=False))

        print(f"Exported {table_name}")
    except Exception as e:
        print(f"Failed to export {table_name}: {e}")

def build_dashboard_index():
    """Builds index.html linking to all monthly (agg) HTML reports."""
    report_dir = "gold/agg/html"
    index_path = os.path.join(report_dir, "index.html")
    links = []
    for file in sorted(os.listdir(report_dir)):
        if file.endswith(".html") and file != "index.html":
            links.append(f'<li><a href="{file}">{file}</a></li>')

    with open(index_path, "w") as f:
        f.write("<h1>Monthly Reports</h1><ul>" + "\n".join(links) + "</ul>")

    print(f"Dashboard index generated: {index_path}")

# Automatically find all dbt_gold views
gold_views = get_gold_views()

# Export all
for view in gold_views:
    export_table(view)

# Build the dashboard index for monthly reports
build_dashboard_index()


