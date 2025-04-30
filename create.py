from pyiceberg.catalog import load_catalog
import pyarrow as pa
import ipyiceberg as ipg
import pandas as pd

props = ipg.cat["catalog"]["default"]
type = props["type"]
uri = props["uri"]
warehouse = props["warehouse"]
py_io = props["py-io-impl"]

catalog = load_catalog(
	name="sqlcat",
	**{"uri": uri,
	   "type": type,
	   "warehouse": warehouse,
	   "py-io-impl": py_io})

# from pyiceberg.schema import Schema
# from pyiceberg.types import NestedField, StringType, DoubleType

# schema = Schema(
#    NestedField(1, "city", StringType(), required=False),
#    NestedField(2, "lat", DoubleType(), required=False),
#    NestedField(3, "long", DoubleType(), required=False),
# )

# tbl = catalog.create_table("docs.cities", schema=schema)

# df = pd.read_csv("/Volumes/DataExports/new-exports/city-gps.txt",
#                 delimiter="|", header=0, names=["city", "lat", "long"])

# df['lat'] = df['lat'].astype(float)
# df['long'] = df['long'].astype(float)
# df.reset_index(drop=True, inplace=True)

table = catalog.load_table("docs.cities")
# pa_df = pa.Table.from_pandas(df=df)
# table.append(df=pa_df)
print(table.scan().to_arrow())
print(table.scan().to_pandas())
