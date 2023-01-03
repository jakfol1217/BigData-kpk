# example submit
"""
spark-submit \
 --packages org.apache.spark:spark-avro_2.12:3.1.2 \
 --master local[1] \
 BigData-kpk/enrichment/enrich_speeds.py \
 20221231
"""

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# Logging configuration

formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)


def fix_schema(df):
    # Incorrectly inferred schema in nifi, but should be fixed for release (Union[float, int], instead of float)
    # Processing for already collected data to start running analysis
    import pyspark
    if type(df.schema["Lon"].dataType) is pyspark.sql.types.DoubleType:
        # schema is correct, just return
        return df
    else:
        # extract union and cast to double
        return df.withColumn(
            "Lon",
            F.when(F.col("Lon.member0").isNull(), F.col("Lon.member1"))
             .otherwise(F.col("Lon.member0"))
        )


def load_all_files_in_dir(spark, directory, hadoop_location="hdfs://localhost:8020"):
    import subprocess

    cmd = ["hdfs", "dfs", "-ls", directory]
    files = subprocess.check_output(cmd).strip().split(b'\n')

    all_dfs = [
        fix_schema(
            spark.read
            .format("avro")
            .load(f'{hadoop_location}{path.decode().split()[-1]}')
        )
        for path in files[1:]
    ]

    combined = all_dfs[0]
    for df in all_dfs[1:]:
        combined = combined.union(df)
    return combined


def make_prev(column):
    from pyspark.sql import Window
    return F.lag(column, 1).over(Window.orderBy("Time").partitionBy("VehicleNumber"))


def enrich_prev(df):
    return (
        df
        .withColumn(
            "Time",
            F.to_timestamp("Time", "yyyy-MM-dd HH:mm:ss")
        )
        .withColumn(
            "PrevTime",
            make_prev("Time")
        )
        .withColumn(
            "PrevLon",
            make_prev("Lon")
        )
        .withColumn(
            "PrevLat",
            make_prev("Lat")
        )
    )


def equirectangular_euclidean_distance(lat1, lat2, lon1, lon2):
    from math import pi
    EARTH_RADIUS_METERS = 6371e3

    phi1 = lat1 * pi / 180
    phi2 = lat2 * pi / 180
    lambda_delta = (lon2 - lon1) * pi / 180

    x = lambda_delta * F.cos((phi1 + phi2) / 2)
    y = phi2 - phi1

    return F.sqrt(x * x + y * y) * EARTH_RADIUS_METERS


def enrich_speed(df, distance_function=equirectangular_euclidean_distance):
    return (
        df
        .withColumn(
            "DeltaTime_s",  # seconds
            F.unix_timestamp(F.col("Time")) - F.unix_timestamp(F.col("PrevTime"))
        )
        .withColumn(
            "DeltaDist_m",  # meters
            distance_function(F.col("Lat"), F.col("PrevLat"), F.col("Lon"), F.col("PrevLon"))
        )
        .withColumn(
            "SpeedInstant_mps",  # meters per second
            F.col("DeltaDist_m") / F.col("DeltaTime_s")
        )
    )


def main(date_to_process):
    appname = f"enrich_speeds_{date_to_process}"

    logger.info(f"Starting spark application {appname}")

    spark = (
        SparkSession.builder
        .appName(appname)
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.1.2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    source_path = f"/user/kpk/bus/{date_to_process}"

    logger.info(f"Load/combine all files from {source_path}")
    combined = load_all_files_in_dir(spark, source_path)

    logger.info("Enriching speeds")
    combined_prev = enrich_prev(combined)
    combined_speeds = enrich_speed(combined_prev)
    logger.info(f"Processed {combined_speeds.count()} rows")

    save_location = f"/user/kpk/speeds/{date_to_process}.avro"

    logger.info(f"Writing to {save_location}")
    try:
        combined_speeds.write.format("avro").save(save_location)
    except AnalysisException:
        import datetime
        new_save_location = f"/user/kpk/speeds/backup/{date_to_process}_from_{datetime.datetime.now().strftime('yyyyMMdd_HHmmss')}.avro"
        logger.warning(f"Failed to write to {save_location}, attempting backup write to {new_save_location}")
        combined_speeds.write.format("avro").save(new_save_location)

    logger.info(f"Ending spark application {appname}")
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) == 1:
        logger.error("This application requires a parameter [date to process]")
    date_to_process = sys.argv[1]
    main(date_to_process)
    sys.exit(0)
