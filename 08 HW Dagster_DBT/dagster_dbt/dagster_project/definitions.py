import os
from pathlib import Path
import duckdb
import pandas as pd
import plotly.express as px
from dagster_dbt import (
    DbtCliResource,
    dbt_assets,
    get_asset_key_for_model,
    build_schedule_from_dbt_selection
)
import dagster as dg

# Пути к файлам
project_dir = Path(__file__).parent.parent
dbt_project_dir = project_dir / "basic_dbt_project"
duckdb_path = str(dbt_project_dir / "dev.duckdb")
manifest_path = dbt_project_dir / "target" / "manifest.json"

# Проверка и компиляция dbt проекта
dbt = DbtCliResource(project_dir=dbt_project_dir)
if not manifest_path.exists():
    dbt.cli(["compile"]).wait()
    if not manifest_path.exists():
        raise Exception(f"Manifest file not found at {manifest_path}")

@dg.asset(compute_kind="python")
def raw_customers():
    data = pd.read_csv("https://docs.dagster.io/assets/customers.csv")
    conn = duckdb.connect(duckdb_path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute("CREATE OR REPLACE TABLE raw.raw_customers AS SELECT * FROM data")
    conn.close()
    return len(data)

@dbt_assets(manifest=manifest_path)
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@dg.asset(
    compute_kind="python",
    deps=[get_asset_key_for_model([dbt_models], "customers")]
)
def customer_histogram():
    conn = duckdb.connect(duckdb_path)
    
    df = conn.sql("SELECT customer_id, FIRST_NAME, LAST_NAME FROM customers").df()
    conn.close()
    
    fig = px.histogram(df, x="FIRST_NAME")  
    html_path = str(project_dir / "customer_histogram.html")
    fig.write_html(html_path)
    return html_path

defs = dg.Definitions(
    assets=[raw_customers, dbt_models, customer_histogram],
    resources={"dbt": dbt},
)