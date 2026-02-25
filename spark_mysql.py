from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg
import sys
import os

# =====================================================
# DATABASE CONFIG
# =====================================================

DB_HOST = "192.168.0.170"
DB_PORT = "3306"
DB_NAME = "sales_analytics_db"
DB_USER = "asset"
DB_PASSWORD = "Asset123"
TABLE_NAME = "employees"


# Construct the JDBC URL
jdbc_url = f"jdbc:mysql://{DB_HOST}:{DB_PORT}/{DB_NAME}?useSSL=false&allowPublicKeyRetrieval=true"


# =====================================================
# INITIALIZE SPARK SESSION
# =====================================================
# Note: We match the Master URL found in your screenshot: spark://192.168.0.170:7077
# Redirect stdout/stderr to null during SparkSession creation to suppress JVM noise (including future taskkill)
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
    
    # We also include the Maven coordinate for the MySQL driver (version 8.0.33)
    spark = SparkSession.builder \
        .appName("SalesAnalytics_MySQL_Connection") \
        .master("local[*]") \
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33") \
        .getOrCreate()

finally:
    # Restore standard streams
    os.dup2(saved_stdout, stdout_fd)
    os.dup2(saved_stderr, stderr_fd)
    os.close(saved_stdout)
    os.close(saved_stderr)
    os.close(devnull)

# Suppress usage logging
spark.sparkContext.setLogLevel("ERROR")

# Suppress the specific Windows shutdown errors
# The errors come from ShutdownHookManager (ERROR) and SparkEnv (WARN)
try:
    log4j = spark._jvm.org.apache.log4j
    log4j.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager").setLevel(log4j.Level.OFF)
    log4j.LogManager.getLogger("org.apache.spark.SparkEnv").setLevel(log4j.Level.ERROR)
except Exception:
    pass

print(">>> Spark Session Created Successfully")

try:
    # =====================================================
    # READ DATA FROM MYSQL
    # =====================================================
    spark.catalog.clearCache()

    print(f">>> Attempting to read table: {TABLE_NAME}")

    
    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("query", f"SELECT * FROM {TABLE_NAME}") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()

    print(">>> Data loaded into DataFrame")

    # =====================================================
    # PERFORM OPERATIONS
    # =====================================================
    
    # 1. Print the Schema (Structure of your data)
    print("--- Schema ---")
    df.printSchema()

    # 2. Show the first 20 rows
    print("--- Sample Data ---")
    df.show()

    # 3. Get total row count
    total_rows = df.count()
    print(f"--- Total Records: {total_rows} ---")

    # 4. Example Analytic: Basic Statistics
    # This gives you count, mean, stddev, min, and max for numeric columns
    print("--- Basic Statistics ---")
    df.describe().show()

    # 5. Example SQL Operation (Create Temp View)
    # This allows you to run pure SQL queries on the data
    df.createOrReplaceTempView("sales_view")
    
    # Example: specific SQL query (modify 'column_name' to a real column in your table)
    # sql_results = spark.sql("SELECT * FROM sales_view LIMIT 5")
    # sql_results.show()

except Exception as e:
    print("!!!!!!!! ERROR !!!!!!!!")
    print(str(e))

finally:
    # =====================================================
    # STOP SPARK SESSION
    # =====================================================
    spark.stop()
    print(">>> Spark Session Stopped")

