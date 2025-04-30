import pyarrow as pa
import pandas as pd
from get_catalog import get_catalog
from schema_cols import schema_bool, schema_date, schema_float
import csv
import datetime as datetime
from typing import List

print("Starting")
print(datetime.datetime.now())

target_tables = ["docs.company_schema", "docs.ratio_schema"]
input_data = ["/Volumes/DataExports/new-exports/company-reference.txt",
              "/Volumes/DataExports/new-exports/key-metrics-ttm.txt"]
schema_float = schema_float()
schema_date = schema_date()
schema_bool = schema_bool()
row_list: List = []

for table, input_file in zip(target_tables, input_data):
    with open(input_file, 'r') as f:
        reader = csv.reader(f, delimiter='|')

        # Read header and create mapping
        catalog = get_catalog()
        table = catalog.load_table(table)
        target_columns = table.schema().column_names
        header = next(reader)
        column_mapping = {col: header.index(col) for col in target_columns if col in header}
        print(f"Processing {input_file}")

        # Process and write data rows
        for row in reader:
            # Schema validation against target columns
            if len(row) == len(column_mapping):
                reordered_row = [row[column_mapping[col]] if col in column_mapping else ''
                                 for col in target_columns]
                row_list.append(reordered_row)
            else:
                print(f"Skipping row: {row}")

        df = pd.DataFrame.from_records(row_list, columns=target_columns, coerce_float=True)
        for col in df.columns:
            if df[col].dtype == "object" and col in schema_float:
                df[col] = df[col].replace("", 0, regex=True).replace(",", "", regex=True).astype(float)
            elif df[col].dtype == "object" and col in schema_date:
                df[col] = pd.to_datetime(df[col]).dt.date
            elif df[col].dtype == "object" and col in schema_bool:
                df[col] = df[col].astype(bool)

        df.fillna(0, inplace=True)
        pa_table = pa.Table.from_pandas(df, schema=table.schema().as_arrow())
        print("Appending rows to table")
        table.append(pa_table)

        print("Ending")
        print(datetime.datetime.now())

