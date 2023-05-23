from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import os

access_key = os.environ.get('AWS_ACCESS_KEY')
secret_key = os.environ.get('AWS_SECRET_KEY')

# Create a Spark session
spark = (SparkSession.builder
    .appName("Raw ETL")
    .config('spark.extraListeners', 'io.openlineage.spark.agent.OpenLineageSparkListener')
    .config('spark.openlineage.host', 'http://marquez-api:5000')
    .config('spark.openlineage.namespace', 'spark_integration')
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
    .config("spark.hadoop.fs.s3a.access.key", access_key)
    .config("spark.hadoop.fs.s3a.secret.key", secret_key)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate())

# Read raw data and write to bucket
data = spark.read.option("inferSchema", "true").option("header", "true").csv("s3a://landing-zone/deniro.csv")
transformed = data.withColumn("Actor", lit("Robert De Niro"))
transformed.write\
           .format('csv')\
           .option('header','true')\
           .save('s3a://raw/raw_data.csv',mode='overwrite')

# Create mock data and write to bucket
mock = [(1980,10), 
        (1984,20), 
        (1988,30), 
        (1992,40),
        (1996,50) 
      ]
mockColumns = ["Year","Val"]
mockDF = spark.createDataFrame(data=mock, schema = mockColumns)

mockDF.write.format('csv')\
      .option('header','true')\
      .save('s3a://raw/extra_data.csv', mode='overwrite')