from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, to_date, lit, struct

# Create a Spark session
spark = SparkSession.builder \
    .appName("Log Transformation") \
    .getOrCreate()

# Define default values for missing columns
DEFAULT_VALUES = {
    "data_event_params_exploded_avg_key_stroke_time": 0,
    "data_event_params_exploded_language_id": 0,
    "data_event_params_exploded_language_version": 0,
    "data_event_params_exploded_layout_id": 0,
    "data_event_params_exploded_numeric_emoji_number_row": 0,
    "data_event_params_exploded_package_name": "unknown",
    "data_event_params_exploded_session_id": "unknown",
    "data_event_params_exploded_theme_id": 0,
    "data_event_params_exploded_top_keys_used": 0,
    "data_event_params_exploded_word_count": 0
}

# Function to transform DataFrame with necessary transformations
def transform_df(df, process_date):
    """
    Transforms the input DataFrame by selecting specific columns, handling timestamp conversion,
    structuring nested fields, and filling missing values with defaults.
    """
    return df.select(
        col("device_id"),
        col("advertisingId"),
        col("app_id"),
        col("app_version"),
        col("sdk_version"),
        col("ip"),
        # Convert string to timestamp
        unix_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ss").alias("event_timestamp"),
        col("event_action").alias("event_name"),
        lit(process_date).alias("process_date"),
        # Convert string to date
        to_date(col("created_at"), "yyyy-MM-dd HH:mm:ss").alias("event_date"),
        col("event_type"),
        col("screen_at"),
        
        # Struct to hold nested fields for event parameters
        struct(
            col("data.avg_key_stroke_time").alias("avg_key_stroke_time"),
            col("data.language_id").alias("language_id"),
            col("data.language_version").alias("language_version"),
            col("data.layout_id").alias("layout_id"),
            col("data.numeric_emoji_number_row").alias("numeric_emoji_number_row"),
            col("data.package_name").alias("package_name"),
            col("data.session_id").alias("session_id"),
            col("data.theme_id").alias("theme_id"),
            col("data.top_keys_used").alias("top_keys_used"),
            col("data.word_count").alias("word_count")
        ).alias("data_event_params"),
        
        # Explode nested fields into individual columns for further processing
        col("data.avg_key_stroke_time").alias("data_event_params_exploded_avg_key_stroke_time"),
        col("data.language_id").alias("data_event_params_exploded_language_id"),
        col("data.language_version").alias("data_event_params_exploded_language_version"),
        col("data.layout_id").alias("data_event_params_exploded_layout_id"),
        col("data.numeric_emoji_number_row").alias("data_event_params_exploded_numeric_emoji_number_row"),
        col("data.package_name").alias("data_event_params_exploded_package_name"),
        col("data.session_id").alias("data_event_params_exploded_session_id"),
        col("data.theme_id").alias("data_event_params_exploded_theme_id"),
        col("data.top_keys_used").alias("data_event_params_exploded_top_keys_used"),
        col("data.word_count").alias("data_event_params_exploded_word_count")
    ).fillna(DEFAULT_VALUES)

# Function to load log files
def load_log_file(file_path):
    """
    Loads a log file into a DataFrame. Logs any loading errors for troubleshooting.
    """
    try:
        return spark.read.json(file_path)
    except Exception as e:
        print(f"Error loading file {file_path}: {e}")
        return None

# Define process date
PROCESS_DATE = "2024-07-01"

# Load all log files into DataFrames
log_files = [
    "/content/Bobble/log 1.log",
    "/content/Bobble/log 2.log",
    "/content/Bobble/log 3.log",
    "/content/Bobble/log 4.log"
]

# Transform DataFrames using the transform_df function
transformed_dfs = []
for log_file in log_files:
    df = load_log_file(log_file)
    if df is not None:
        transformed_dfs.append(transform_df(df, PROCESS_DATE))

# Combine all transformed DataFrames into one
df_combined = transformed_dfs[0]
for df in transformed_dfs[1:]:
    df_combined = df_combined.unionByName(df)

# Write the combined DataFrame to a single Parquet file
output_path = "/content/final_output.parquet"
df_combined.coalesce(1).write.mode("overwrite").parquet(output_path)

print("Data transformation and export complete.")
