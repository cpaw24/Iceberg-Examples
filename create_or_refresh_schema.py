from get_catalog import get_catalog
from schema_def import company_schema, ratio_schema_ttm, forecast_schema

catalog = get_catalog()

schema_list = [company_schema, ratio_schema_ttm, forecast_schema]
table_list = ["company", "ratios_ttm", "forecasts"]

try:
   for schema, tab in zip(schema_list, table_list):

       catalog.drop_table(identifier=f"docs.{tab}")
       print(f"Dropped table: docs.{tab}")

       catalog.create_table_if_not_exists(identifier=f"docs.{tab}", schema=schema)
       print(f"Created table: docs.{tab}")

except Exception as e:
   print(e)
