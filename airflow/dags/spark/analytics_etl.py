from pyspark.sql import SparkSession
import os

access_key = os.environ.get('AWS_ACCESS_KEY')
secret_key = os.environ.get('AWS_SECRET_KEY')

# Create a Spark session
spark = (SparkSession.builder
    .appName("Analytics ETL")
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

final_data = spark.read.option("inferSchema", "true").option("header", "true").csv("s3a://transformed/transformed_data.csv")
analytics_data = final_data.where(final_data['"Score"'] >= 90)

analytics_data.write.format('csv').option('header','true').save('s3a://analytics/analytics_data.csv',mode='overwrite')