import pyarrow as pa
import pandas as pd
from get_catalog import get_catalog
from schema_cols import schema_bool, schema_float, schema_int, schema_date, schema_datetime
import csv
import datetime as datetime
from typing import List

pd.options.display.max_rows = 50
pd.options.display.max_columns = 50
pd.options.display.width = 1000
pd.options.display.float_format = '{:,.2f}'.format

print("Starting")
print(datetime.datetime.now())

# Target tables; repeat for each year
target_tables = ["docs.company", "docs.ratios_ttm",
                 "docs.forecasts", "docs.forecasts", "docs.forecasts", "docs.forecasts", "docs.forecasts",
                 "docs.forecasts", "docs.forecasts", "docs.forecasts", "docs.forecasts", "docs.forecasts",
                 "docs.forecasts"]

# Data files are pipe-delimited and quoted with double quotes
input_data = ["/Volumes/DataExports/new-exports/company-reference.txt",  "/Volumes/DataExports/new-exports/key-ratios-ttm.txt",
              "/Volumes/DataExports/new-exports/forecast-2015.txt", "/Volumes/DataExports/new-exports/forecast-2016.txt",
              "/Volumes/DataExports/new-exports/forecast-2017.txt", "/Volumes/DataExports/new-exports/forecast-2018.txt",
              "/Volumes/DataExports/new-exports/forecast-2019.txt", "/Volumes/DataExports/new-exports/forecast-2020.txt",
              "/Volumes/DataExports/new-exports/forecast-2021.txt", "/Volumes/DataExports/new-exports/forecast-2022.txt",
              "/Volumes/DataExports/new-exports/forecast-2023.txt", "/Volumes/DataExports/new-exports/forecast-2024.txt",
              "/Volumes/DataExports/new-exports/forecast-2025.txt"]

# Get schema column types
schema_float = schema_float()
schema_date = schema_date()
schema_bool = schema_bool()
schema_int = schema_int()
schema_datetime = schema_datetime()
catalog = get_catalog()
row_list: List = []


for table, input_file in zip(target_tables, input_data):
    with open(input_file, 'r') as f:
        reader = csv.reader(f, delimiter='|', lineterminator='\n', quotechar='\"', doublequote=True)

        # Read header and create mapping
        table = catalog.load_table(table)
        target_columns = table.schema().column_names
        header = next(reader)

        # Create mapping of target columns to header indices
        column_mapping = {col: header.index(col) for col in target_columns if col in header}
        print(f"Processing {input_file}")

        # Process and write data rows
        for row in reader:
            # Basic Schema validation against target columns
            if len(row) == len(column_mapping):
                # Re-order columns based on mapping to schema
                reordered_row = [row[column_mapping[col]] if col in column_mapping else ''
                                 for col in target_columns]
                row_list.append(reordered_row)
            else:
                print(f"Skipping row: {row}")

        # Create a Pandas dataframe and apply data types for each column in the lists
        df = pd.DataFrame.from_records(row_list, columns=target_columns, coerce_float=True)
        # Type conversion
        for col in df.columns:
            if (df[col].dtype == "object") and (col in schema_date):
                df[col] = pd.to_datetime(df[col], "coerce").dt.date
            elif (df[col].dtype == "object") and (col in schema_datetime):
                # Iceberg doesn't like nanoseconds at the time of this code, but you can us millisecond and microsecond precision
                df[col] = pd.to_datetime(df[col], "coerce").astype('datetime64[s]')
            elif (df[col].dtype == "object") and (col in schema_float):
                df[col] = df[col].replace("", 0, regex=True).replace(",", "", regex=True).astype(float)
            elif (df[col].dtype == "object") and (col in schema_bool):
                df[col] = df[col].astype(bool)
            elif (df[col].dtype == "object") and (col in schema_int):
                df[col] = df[col].astype(int)

        # Get rid of NaN values
        df.fillna(0, inplace=True)
        print(df.head())
        # Create pyarrow table/dataframe from pandas dataframe
        pa_table = pa.Table.from_pandas(df=df, schema=table.schema().as_arrow(), safe=False)
        print("Appending rows to table")
        table.append(pa_table)

        print("Ending")
        print(datetime.datetime.now())
        # Reset lists for the next file
        row_list = []
        reordered_row = []
        df = []
        column_mapping = []

