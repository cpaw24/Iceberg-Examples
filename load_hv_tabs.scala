val builder = SparkSession
      .builder()
      .appName("Python Spark SQL Hive integration example") \
      .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.local","org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.local.type","hive")
      .config("spark.sql.catalogImplementation", "hive")
      .config("spark.sql.hive.metastore.jars", "maven")
	  .config("spark.sql.hive.metastore.version", "4.0.0")
	  .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083")
      .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
      .getOrCreate()
