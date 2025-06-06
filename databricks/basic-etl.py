from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# -----------------------------
# 1. Create Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("Basic_ETL") \
    .getOrCreate()

# -----------------------------
# 2. Extract: Load CSV data
# -----------------------------
input_path = "data/employees.csv"  # Your input file (sample: name, age, salary)
df = spark.read.option("header", True).csv(input_path)

print("== Raw Data ==")
df.show()

# -----------------------------
# 3. Transform: Clean and Convert
# -----------------------------
# Drop rows with missing values
df_clean = df.dropna()

# Convert salary to float type
df_clean = df_clean.withColumn("salary", col("salary").cast("float"))

# Filter: keep only salaries > 50,000
df_filtered = df_clean.filter(col("salary") > 50000)

print("== Transformed Data ==")
df_filtered.show()

# -----------------------------
# 4. Load: Save as Parquet (or Delta if you want)
# -----------------------------
output_path = "output/high_salary_employees"
df_filtered.write.mode("overwrite").parquet(output_path)

print(f"ETL complete. Output saved to: {output_path}")
