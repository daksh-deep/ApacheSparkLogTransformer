# Apache Spark Log Transformer 🏆

## Hackathon Winner - Bobble AI Datathon
This project was awarded first place in the Bobble AI Datathon for efficiently transforming large-scale unstructured JSON log files into optimized Parquet format. The solution successfully processed over 5 million JSON rows while maintaining data integrity and implementing robust error handling.

> [!WARNING]
> This code cannot be reproduced in its entirety as it requires specific log files that were provided by Bobble AI during the Datathon. These log files contain confidential information and are not publicly available. The code is shared for educational and reference purposes only.

## Project Overview
This PySpark-based solution transforms unstructured JSON log files into a structured Parquet format, optimizing for both storage and query performance. The transformation process includes timestamp standardization, nested field handling, and default value management for missing data points.

## Team Details

| Name   | LinkedIn                                       | Email                   |
|---------------|------------------------------------------------|-------------------------|
| Daksh Deep    | [linkedin.com/in/daksh-deep-791a1a298/](https://linkedin.com/in/daksh-deep-791a1a298/) | dakshsaxena04@gmail.com |
| Yashvi Arya   | [linkedin.com/in/yashviarya/](https://linkedin.com/in/yashviarya/)               | na                      |

## Key Features
- Efficient processing of large-scale JSON log files (5M+ rows) with minimal memory usage
- Robust error handling for file loading, transformation, and missing data
- Structured organization and flattening of nested JSON fields for easier querying
- Standardized timestamp and date formatting for consistency
- Default value handling for missing or incomplete columns
- Optimized output storage in Parquet format for faster querying and reduced storage footprint

## Technical Architecture
- **Input**: Multiple unstructured JSON log files
- **Processing Engine**: Apache Spark (PySpark)
- **Output**: Single consolidated Parquet file
- **Data Quality**: Built-in null handling and default value assignment

## Prerequisites
- Apache Spark
- Python 3.8
- PySpark SQL
- Sufficient memory to handle large-scale data processing

## Installation
```bash
# Clone the repository
git clone https://github.com/daksh-deep/ApacheSparkLogTransformer

# Install required dependencies
pip install pyspark
```

## Usage
1. Configure your Spark session:
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Log Transformation") \
    .getOrCreate()
```

2. Update the log file paths:
```python
log_files = [
    "/path/to/log1.log",
    "/path/to/log2.log",
    "/path/to/log3.log",
    "/path/to/log4.log"
]
```

3. Set your process date:
```python
PROCESS_DATE = "YYYY-MM-DD"
```

4. Run the transformation:
```python
python log_transformer.py
```

## Data Transformation Details
The transformer handles the following fields:
- Device and advertising IDs
- Application metadata (app_id, version)
- Event timestamps and actions
- User interaction data
- Device settings and configurations

### Default Values
The system implements the following default values for missing data:
```python
DEFAULT_VALUES = {
    "data_event_params_exploded_avg_key_stroke_time": 0,
    "data_event_params_exploded_language_id": 0,
    # ... [other default values]
}
```

## Performance Optimization
- Efficient data reading through Spark's JSON parser
- Optimized schema handling for nested structures
- Coalesced output for improved file organization
- Memory-efficient processing of large datasets

## Error Handling
The solution includes comprehensive error handling:
- File loading validation
- Data transformation error catching
- Null value management
- Process monitoring and logging

## Output
The transformed data is saved as a Parquet file with:
- Optimized columnar storage
- Compressed data format
- Efficient query support
- Schema preservation  

## License
Apache Spark Log Transformer is licensed under the MIT License. See the LICENSE file for more details.

## Acknowledgments
1. Special thanks to Dr. Kavita Jhajharia and Dr. Rahul Saxena from Manipal University (MU), Jaipur, for their constant support.
2. Special thanks to the Department of Information Technology, Manipal University (MU), Jaipur, Bobble AI, and LearnIT MUJ for organizing the Datathon and providing the opportunity to develop this solution.
