from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg
import sys
import os

# =====================================================
# MONGODB CONFIG
# =====================================================
MONGO_HOST = "192.168.0.170"
MONGO_PORT = "27017"
MONGO_DB = "bigdata_db"                 # change if needed
MONGO_COLLECTION = "employees"    # change if needed
MONGO_USER = "asset"
MONGO_PASSWORD = "Asset@12345"  # @ encoded as %40

mongo_uri = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}"

# =====================================================
# INITIALIZE SPARK SESSION
# =====================================================
try:
    sys.stdout.flush()
    sys.stderr.flush()
    devnull = os.open(os.devnull, os.O_WRONLY)
    stdout_fd = sys.stdout.fileno()
    stderr_fd = sys.stderr.fileno()
    saved_stdout = os.dup(stdout_fd)
    saved_stderr = os.dup(stderr_fd)
    os.dup2(devnull, stdout_fd)
    os.dup2(devnull, stderr_fd)

    spark = SparkSession.builder \
        .appName("SalesAnalytics_MongoDB_Connection") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.mongodb.read.connection.uri", mongo_uri) \
        .getOrCreate()

finally:
    os.dup2(saved_stdout, stdout_fd)
    os.dup2(saved_stderr, stderr_fd)
    os.close(saved_stdout)
    os.close(saved_stderr)
    os.close(devnull)

spark.sparkContext.setLogLevel("ERROR")

try:
    log4j = spark._jvm.org.apache.log4j
    log4j.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager").setLevel(log4j.Level.OFF)
    log4j.LogManager.getLogger("org.apache.spark.SparkEnv").setLevel(log4j.Level.ERROR)
except Exception:
    pass

print(">>> Spark Session Created Successfully")

try:
    # =====================================================
    # READ DATA FROM MONGODB
    # =====================================================
    spark.catalog.clearCache()

    print(f">>> Attempting to read collection: {MONGO_COLLECTION}")

    df = spark.read \
        .format("mongo") \
        .option("database", MONGO_DB) \
        .option("collection", MONGO_COLLECTION) \
        .load()

    print(">>> Data loaded into DataFrame")

    # =====================================================
    # PERFORM OPERATIONS
    # =====================================================

    print("--- Schema ---")
    df.printSchema()

    print("--- Sample Data ---")
    df.show()

    total_rows = df.count()
    print(f"--- Total Records: {total_rows} ---")

    print("--- Basic Statistics ---")
    df.describe().show()

    df.createOrReplaceTempView("sales_view")

    # Example SQL
    # spark.sql("SELECT * FROM sales_view LIMIT 5").show()

except Exception as e:
    print("!!!!!!!! ERROR !!!!!!!!")
    print(str(e))

finally:
    spark.stop()
    print(">>> Spark Session Stopped")

