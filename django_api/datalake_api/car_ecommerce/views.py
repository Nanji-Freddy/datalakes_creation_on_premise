# from rest_framework.views import APIView
# from rest_framework.response import Response
# from minio import Minio
# from pyspark.sql import SparkSession
# import os

# # Initialize MinIO client
# client = Minio(
#     os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
#     access_key=os.getenv('MINIO_ACCESS_KEY', 'datalake'),
#     secret_key=os.getenv('MINIO_SECRET_KEY', 'datalakekey'),
#     secure=False
# )

# # Initialize Spark session directly
# def get_spark_session():
#     def configure_s3(spark_session, s3_endpoint, s3_access_key, s3_secret_key):
#         hadoop_conf = spark_session.sparkContext._jsc.hadoopConfiguration()
#         hadoop_conf.set("fs.s3a.access.key", s3_access_key)
#         hadoop_conf.set("fs.s3a.secret.key", s3_secret_key)
#         hadoop_conf.set("fs.s3a.endpoint", s3_endpoint)
#         hadoop_conf.set("fs.s3a.path.style.access", "true")
#         hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
#         hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

#     # Configure Spark session
#     packages = [
#         "org.apache.hadoop:hadoop-aws:3.3.4",
#         "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12-1.5.2",
#         "com.amazonaws:aws-java-sdk-bundle:1.12.262"
#     ]

#     spark = (
#         SparkSession.builder.appName("MageSparkSession")
#         .config("spark.jars.packages", ",".join(packages))
#         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
#         .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
#         .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
#         .config("spark.sql.catalog.local.type", "hadoop")
#         .config("spark.sql.catalog.local.warehouse", "s3a://iceberg-demo-bucket/warehouse")
#         .config("spark.ui.port", "4050")
#         .getOrCreate()
#     )
    
# /home/freddy/Documents/Cours_efrei/Data_Lakes/TP/nouveau_projet_datalake/datalakes_creation_on_premise/mage_demo/spark-config/aws-java-sdk-bundle-1.12.262.jar
#     # Configure S3 settings
#     configure_s3(
#         spark,
#         s3_endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
#         s3_access_key=os.getenv("MINIO_ACCESS_KEY", "datalake"),
#         s3_secret_key=os.getenv("MINIO_SECRET_KEY", "datalakekey"),
#     )

#     return spark


# class TableDataView(APIView):
#     def get(self, request, table_name):
#         try:
#             # Initialize Spark session
#             spark = get_spark_session()

#             # Read data from Iceberg table
#             df = spark.table(f"local.iceberg_demo.{table_name}")

#             # Convert to Pandas DataFrame and return as JSON
#             data = df.toPandas().to_dict(orient="records")
#             return Response(data)
#         except Exception as e:
#             return Response({"error": str(e)}, status=500)



from rest_framework.views import APIView
from rest_framework.response import Response
from minio import Minio
from pyspark.sql import SparkSession
import os

# Initialize MinIO client
client = Minio(
    os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY", "datalake"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "datalakekey"),
    secure=False,
)

# Initialize Spark session directly
def get_spark_session():
    def configure_s3(spark_session, s3_endpoint, s3_access_key, s3_secret_key):
        hadoop_conf = spark_session.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.access.key", s3_access_key)
        hadoop_conf.set("fs.s3a.secret.key", s3_secret_key)
        hadoop_conf.set("fs.s3a.endpoint", s3_endpoint)
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Configure Spark session
    jars = [
        "/home/freddy/Documents/Cours_efrei/Data_Lakes/TP/nouveau_projet_datalake/datalakes_creation_on_premise/mage_demo/spark-config/aws-java-sdk-bundle-1.12.262.jar",
        "/home/freddy/Documents/Cours_efrei/Data_Lakes/TP/nouveau_projet_datalake/datalakes_creation_on_premise/mage_demo/spark-config/iceberg-spark-runtime-3.5_2.12-1.5.2.jar",  # Replace with your actual path
        "/home/freddy/Documents/Cours_efrei/Data_Lakes/TP/nouveau_projet_datalake/datalakes_creation_on_premise/mage_demo/spark-config/bundle-2.26.15.jar",  # Replace with your actual path
        "/home/freddy/Documents/Cours_efrei/Data_Lakes/TP/nouveau_projet_datalake/datalakes_creation_on_premise/mage_demo/spark-config/delta-core_2.12-2.4.0.jar",  # Replace with your actual path
        "/home/freddy/Documents/Cours_efrei/Data_Lakes/TP/nouveau_projet_datalake/datalakes_creation_on_premise/mage_demo/spark-config/delta-storage-2.4.0.jar",  # Replace with your actual path
        "/home/freddy/Documents/Cours_efrei/Data_Lakes/TP/nouveau_projet_datalake/datalakes_creation_on_premise/mage_demo/spark-config/hadoop-aws-3.3.4.jar",  # Replace with your actual path
        "/home/freddy/Documents/Cours_efrei/Data_Lakes/TP/nouveau_projet_datalake/datalakes_creation_on_premise/mage_demo/spark-config/url-connection-client-2.26.15.jar",  # Replace with your actual path
    ]

    spark = (
        SparkSession.builder.appName("MageSparkSession")
        .config("spark.jars", ",".join(jars))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "s3a://iceberg-demo-bucket/warehouse")
        .config("spark.ui.port", "4050")
        .getOrCreate()
    )

    # Configure S3 settings
    configure_s3(
        spark,
        s3_endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        s3_access_key=os.getenv("MINIO_ACCESS_KEY", "datalake"),
        s3_secret_key=os.getenv("MINIO_SECRET_KEY", "datalakekey"),
    )

    return spark


class TableDataView(APIView):
    def get(self, request, table_name):
        try:
            # Initialize Spark session
            spark = get_spark_session()

            # Read data from Iceberg table
            df = spark.table(f"local.iceberg_demo.{table_name}")

            # Convert to Pandas DataFrame and return as JSON
            data = df.toPandas().to_dict(orient="records")
            return Response(data)
        except Exception as e:
            return Response({"error": str(e)}, status=500)
