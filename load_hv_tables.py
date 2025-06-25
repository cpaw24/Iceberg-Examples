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
         .config("spark.sql.catalog.local.warehouse", "hdfs://10.0.0.39:9000/user/hive/warehouse")
         .config("spark.sql.defaultCatalog", "local")
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
		 .config("spark.sql.catalogImplementation", "hive")
         .config("spark.hadoop.hive.metastore.uris", "thrift://10.0.0.39:9083")
         .config("spark.sql.warehouse.dir", "hdfs://10.0.0.39:9000/user/hive/warehouse")
         .config("spark.sql.warehouse.external.enabled", "true")
         .enableHiveSupport()
         .getOrCreate())

# spark is an existing SparkSession
hive_jdbc = {
	"url": "jdbc:hive2://10.0.0.39:10001/default",
	"driver": "org.apache.hive.jdbc.HiveDriver",
	"table": "company_stocks"
}

# Read from Hive using JDBC
dfh = spark.read.jdbc(url=hive_jdbc.get("url"), table=hive_jdbc.get("table"))
print(dfh.schema.fieldNames())

df = spark.read.csv('hdfs://10.0.0.39:9000/input_files/company-reference.txt',
                                   header=True, inferSchema=True, sep='|')

# Fixing dataframe types
df_new = df.withColumn("AsOfDate", col("AsOfDate").cast(TimestampType())) \
  	.withColumn("ExchangeShortname", col("ExchangeShortName").cast(StringType())) \
  	.withColumn("Employees", col("Employees").cast(IntegerType())) \
  	.withColumn("ADR", col('ADR').cast(BooleanType())) \
  	.withColumn("isFund", col('isFund').cast(BooleanType())) \
  	.withColumn("isETF", col('isETF').cast(BooleanType())) \
  	.withColumn("AverageVolume", col("AverageVolume").cast(DoubleType())) \
  	.withColumn("Beta", col("Beta").cast(DoubleType())) \
  	.withColumn("Discounted_Cash_Flow", col("Discounted_Cash_Flow").cast(DoubleType())) \
  	.withColumn("Discounted_Cash_Flow_Diff", col("Discounted_Cash_Flow_Diff").cast(DoubleType())) \
  	.withColumn("IPO_Date", col("IPO_Date").cast(DateType()))
print(df.printSchema())

#Fixing column order
df_new = df_new.select(col("AsOfDate"), col("CompanyName"), col("Ticker"), col("CIK"), col("ISIN"), col("CUSIP"),
                         col("Description"),col("Exchange"), col("ExchangeShortName"), col("Sector"), col("Country"),
                         col("Currency"), col("City"), col("State"), col("Address"), col("Employees"), col("CEO"),
                         col("Website"), col("ZipCode"), col("ADR"), col("isFund"), col("isETF"), col("AverageVolume"),
                         col("Beta"), col("Discounted_Cash_Flow"), col("Discounted_Cash_Flow_Diff"), col("IPO_Date"),
                         col("Industry"))

# df_new.createOrReplaceTempView("vw_raw_company_stocks")
df_new.write.mode("append").insertInto("default.raw_company_stocks")

spark.sql("insert into default.company_stocks "
             "select AsOfDate, CompanyName, StockSymbol, CIK, ISIN, CUSIP, Description, `Exchange`, `ExchangeShortName`, Sector, "
             "Country, Currency, City, State, Address, Employees, CEO, Website, ZipCode, ADR, "
             "isFund, isETF, AverageVolume, "
             "Beta, Discounted_Cash_Flow, Discounted_Cash_Flow_Diff, "
             "IPO_Date, Industry from default.raw_company_stocks")


print("Company Stocks")
spark.sql("select * from default.company_stocks").show()

# Connect to Hive using Metastore Client & Thrift
hive_con = hme.HiveMetastoreClient(host="10.0.0.39", port=9083)
hive_client = hme.HiveMetastoreClient.open(hive_con)
hme_df = hive_client.get_table(dbname="default", tbl_name="raw_company_stocks")
print(hme_df)

