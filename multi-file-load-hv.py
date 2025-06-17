from datetime import datetime

from pyspark.sql.types import DoubleType, DateType, BooleanType, IntegerType, StringType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import hive_metastore_client as hme

spark = (SparkSession
         .builder
         .appName("Python Spark SQL Hive integration example")
		 .master("local[*]")
         .config("spark.jars", "/Volumes/ExtShield/opt/spark-3.5.6-bin-hadoop3/jars/iceberg-spark-runtime-3.5_2.12-1.9.1.jar")
         .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1")
         .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.local.type", "hive")
         .config("spark.sql.catalog.local.warehouse", "hdfs://localhost:9000/user/hive/warehouse")
         .config("spark.sql.defaultCatalog", "local")
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
		 .config("spark.sql.catalogImplementation", "hive")
         .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083")
         .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
         .config("spark.sql.warehouse.external.enabled", "true")
         .enableHiveSupport()
         .getOrCreate())


df = spark.read.csv('hdfs://localhost:9000/input_files/forecast-*.txt',
                                   header=True, inferSchema=True, sep='|')

# spark is an existing SparkSession
hive_jdbc = {
	"url": "jdbc:hive2://localhost:10001/default;transportMode=http;httpPath=cliservice",
	"driver": "org.apache.hive.jdbc.HiveDriver",
	"table": "forecasts"
}

existing_df = spark.read.jdbc(url=hive_jdbc.get("url"), table=hive_jdbc.get("table"))
target_schema = existing_df.schema.names
target_columns = existing_df.schema.fieldNames()
target_columns = [c.replace("forecasts.", "") for c in target_columns]
target_table = hive_jdbc.get("table")

# Create a mapping between upper and lower case column names
column_mapping = {col_name: col_name for col_name in target_columns}

# First rename the columns to match case
for old_col in df.columns:
    if old_col.lower() in column_mapping:
        print(f"{old_col}, {old_col.lower()}")
        df = df.withColumnRenamed(old_col, column_mapping[old_col.lower()])

print(df.printSchema())
# Reorder columns to match existing_df
df = df.select(target_columns)
print(df.count())

# df.write.mode("append").insertInto("default.forecasts")

forecast_df = spark.read.table("default.forecasts")
company_stocks_df = spark.read.table("default.company_stocks")

jn_df = forecast_df.join(company_stocks_df, [forecast_df.ticker == company_stocks_df.stocksymbol, forecast_df.calendaryear==2024], how="inner")
selected_df = jn_df.select("ticker", "calendaryear", "exchange", "sector", "isin", "debt_growth_pct",
                           "dividend_per_share_growth", "ebitda_growth_pct", "eps_diluted_growth_pct", "eps_growth_pct",
                           "cash_per_share", "revenue_growth_pct", "net_income_growth_pct")

selected_df.write.saveAsTable("default.company_forecasts_2024", format="iceberg", mode="overwrite")

print(jn_df.count())
print(jn_df.show())


