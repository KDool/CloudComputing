import sys
import csv
from pyspark.sql import SparkSession

def parse_csv_line(line):
    try:
        reader = csv.reader([line])
        return next(reader)
    except:
        return []

def main():
    # Create Spark Context
    spark = SparkSession.builder \
        .appName("Chicago Crimes Analytics Spark RDD") \
        .getOrCreate()
    
    sc = spark.sparkContext

    if len(sys.argv) != 3:
        print("Usage: spark-submit chicago_crimes_spark_rdd.py <input_dir_or_csv> <final_output>")
        sys.exit(-1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    raw_rdd = sc.textFile(input_path)


    data_rdd = raw_rdd.filter(lambda line: "Primary Type" not in line)

    # JOB 1 
    def mapper_job1(line):
        fields = parse_csv_line(line)
        
        if len(fields) >= 10:
            primary_type = fields[6].strip().upper()
            location = fields[8].strip().upper()
            arrest_str = fields[9].strip().lower()

            if primary_type and location:
                is_arrested = 1 if arrest_str == 'true' else 0
                return ((primary_type, location), (is_arrested, 1))
        return None


    job1_rdd = data_rdd.map(mapper_job1) \
                       .filter(lambda x: x is not None) \
                       .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # JOB 2
    def mapper_job2(record):
        primary_type = record[0][0]
        location = record[0][1]
        arrests = record[1][0]
        totals = record[1][1]

        rate = (arrests / totals) * 100.0 if totals > 0 else 101.0

        return (primary_type, (location, rate, totals))

    def find_worst_location(a, b):
        return a if a[1] < b[1] else b

    job2_rdd = job1_rdd.filter(lambda x: x[1][1] > 10) \
                       .map(mapper_job2) \
                       .reduceByKey(find_worst_location)

    # FORMAT and Write result to HDFS
    def format_output(record):
        primary_type = record[0]
        location = record[1][0]
        rate = record[1][1]
        totals = record[1][2]
        
        return f"{primary_type}\tWorst Location: {location:<25} (Arrest Rate: {rate:5.2f}%) | Total City Incidents: {totals}"

    final_output_rdd = job2_rdd.map(format_output)

    final_output_rdd.coalesce(1).saveAsTextFile(output_path)

    print("Spark RDD Analytics Completed Successfully!")
    spark.stop()

if __name__ == "__main__":
    main()