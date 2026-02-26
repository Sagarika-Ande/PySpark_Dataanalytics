from pyspark.sql import SparkSession
import sys
import os

# =====================================================
# DATABASE CONFIG
# =====================================================

DB_HOST = "192.168.0.170"
DB_PORT = "3306"
DB_NAME = "bigdata"          # change database here
DB_USER = "asset"
DB_PASSWORD = "Asset123"
TABLE_NAME = "transactions"  # change table here

# JDBC URL
jdbc_url = (
    f"jdbc:mysql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    "?useSSL=false&allowPublicKeyRetrieval=true"
)

print("=================================================")
print("Reading Database :", DB_NAME)
print("Reading Table    :", TABLE_NAME)
print("=================================================")

# =====================================================
# CREATE SPARK SESSION
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

    spark = (
        SparkSession.builder
        .appName("MySQL_Spark_Reader")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "mysql:mysql-connector-java:8.0.33"
        )
        .getOrCreate()
    )

finally:
    os.dup2(saved_stdout, stdout_fd)
    os.dup2(saved_stderr, stderr_fd)
    os.close(saved_stdout)
    os.close(saved_stderr)
    os.close(devnull)

spark.sparkContext.setLogLevel("ERROR")

print(">>> Spark Session Created Successfully")

try:
    # =====================================================
    # CLEAR CACHE (IMPORTANT)
    # =====================================================
    spark.catalog.clearCache()
    spark.sql("CLEAR CACHE")

    # =====================================================
    # READ TABLE FROM MYSQL
    # =====================================================
    print(f">>> Loading table {TABLE_NAME} from {DB_NAME}")

    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"{DB_NAME}.{TABLE_NAME}")   # ✅ FIXED
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .load()
    )

    print(">>> Data Loaded Successfully")

    # =====================================================
    # DATA OPERATIONS
    # =====================================================

    print("\n--- Schema ---")
    df.printSchema()

    print("\n--- Sample Data ---")
    df.show(20, False)

    print("\n--- Total Records ---")
    total_rows = df.count()
    print("Total Rows :", total_rows)

    print("\n--- Basic Statistics ---")
    df.describe().show()

    # SQL operations
    df.createOrReplaceTempView("mysql_view")

    print("\n--- SQL Preview ---")
    spark.sql("SELECT * FROM mysql_view LIMIT 5").show()

except Exception as e:
    print("\n!!!!!!!! ERROR !!!!!!!!")
    print(str(e))

finally:
    # =====================================================
    # STOP SPARK
    # =====================================================
    spark.stop()
    print(">>> Spark Session Stopped")