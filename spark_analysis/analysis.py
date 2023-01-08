# example submit
"""
spark-submit \
 --packages org.apache.spark:spark-avro_2.12:3.1.2 \
 --master local[1] \
 analysis.py \
 20221231
"""

import sys
import logging
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Logging configuration

formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

def main(date_to_process):
    spark = (
        SparkSession.builder
        .appName("CalculateStats")
        .config('spark.jars.packages', "org.apache.spark:spark-avro_2.12:3.1.2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Starting spark application")

    source_path = f"/user/kpk/speeds/{date_to_process}.avro"

    logger.info(f"Load/combine all files from {source_path}")
    combined = spark.read.format("avro").load(f"hdfs://localhost:8020{source_path}")

#---------------------------------------------------------------------------------------------------

    logger.info("Counting statistics")
    combined.createOrReplaceTempView("Combined_sql")
    veh_stats = spark.sql("SELECT Lines, VehicleNumber, Brigade, SUM(DeltaDist_m)/1000 AS TotalDist_km, SUM(DeltaTime_s)/3600 AS TotalTime_h, AVG(NULLIF(SpeedInstant_mps,0)) AS AvgSpeed_mps FROM Combined_sql GROUP BY Lines, VehicleNumber, Brigade")
#---^^^ ramka do hbase

    veh_stats.createOrReplaceTempView("VehStats_sql")
    line_stats = spark.sql("SELECT Lines, SUM(TotalDist_km) AS TotalDistLine_km, SUM(TotalTime_h) AS TotalTimeLine_h, AVG(NULLIF(AvgSpeed_mps,0)) AS AvgSpeedLine_mps FROM VehStats_sql GROUP BY Lines")
#---^^^ ramka do hbase

    # path1 = f"hdfs://localhost:8020/user/kpk/reports/veh_stats_{date_to_process}.json"
    # path2 = f"hdfs://localhost:8020/user/kpk/reports/line_stats_{date_to_process}.json"
    # logger.info("Saving files...")
    # veh_stats.write.json(path1)
    # line_stats.write.json(path2)
    # logger.info("Files saved")


    #joining with weather dataframe
    combined_with_year = spark.sql("SELECT *, MONTH(Time) AS Month_a, DAY(Time) AS Day_a, HOUR(Time) AS Hour_a FROM Combined_sql")
    year = int(str(date_to_process)[:4])
    weather_path = f"hdfs://localhost:8020/user/kpk/weather/{year}/{year}.csv"
    df_weather = spark.read.option("header","true").option("inferschema","true").csv(weather_path)
    df_weather.createOrReplaceTempView("Weather_sql")
    weather_with_day = spark.sql("SELECT *, MONTH(Date) AS Month, DAY(Date) AS Day FROM Weather_sql")


    bus_weather = combined_with_year.join(weather_with_day, (combined_with_year["Month_a"] == weather_with_day["Month"]) &
               ( combined_with_year["Day_a"] == weather_with_day["Day"]) &
               ( combined_with_year["Hour_a"] == weather_with_day["Hour"])).drop("Day_a", "Hour_a", "Month_a")
    bus_weather.createOrReplaceTempView("BusWeather_sql")
    bus_weather_rain = spark.sql("SELECT CASE WHEN prcp > 0 THEN 1 ELSE 0 END AS Rain, * FROM BusWeather_sql")

    #bus-weather stats
    bus_weather_rain.createOrReplaceTempView("BusWeatherRain_sql")
    bus_weather_stats = spark.sql("SELECT Lines, VehicleNumber, Brigade, coco, SUM(DeltaDist_m)/1000 AS TotalDist_km, SUM(DeltaTime_s)/3600 AS TotalTime_h, AVG(NULLIF(SpeedInstant_mps,0)) AS AvgSpeed_mps FROM BusWeatherRain_sql GROUP BY Lines, VehicleNumber, Brigade, coco")
#---^^^ ramka do hbase

    # path3 = f"hdfs://localhost:8020/user/kpk/reports/veh_weather_stats_{date_to_process}.json"
    # logger.info("Saving files...")
    # bus_weather_stats.write.json(path3)
    # logger.info("Files saved")

    bus_weather_rain_stats = spark.sql("SELECT Lines, VehicleNumber, Brigade, Rain, SUM(DeltaDist_m)/1000 AS TotalDist_km, SUM(DeltaTime_s)/3600 AS TotalTime_h, AVG(NULLIF(SpeedInstant_mps,0)) AS AvgSpeed_mps FROM BusWeatherRain_sql GROUP BY Lines, VehicleNumber, Brigade, Rain")

    bus_weather_rain_stats.createOrReplaceTempView("BusWeatherStats_sql")
    bus_weather_line_stats = spark.sql("SELECT Lines, Rain, SUM(TotalDist_km) AS TotalDistLine_km, AVG(TotalDist_km) AS AvgDistLine_km, AVG(TotalTime_h) AS AvgTimeLine_h, AVG(NULLIF(AvgSpeed_mps,0)) AS AvgSpeedLine_mps FROM BusWeatherStats_sql GROUP BY Lines, Rain")
#---^^^ ramka do hbase

    # path4 = f"hdfs://localhost:8020/user/kpk/reports/line_weather_stats_{date_to_process}.json"
    # logger.info("Saving files...")
    # bus_weather_line_stats.write.json(path4)
    # logger.info("Files saved")

    logger.info("The end")


if __name__ == "__main__":
    if len(sys.argv) == 1:
        logger.error("This application requires a parameter [date to process]")
    date_to_process = sys.argv[1]
    main(date_to_process)
    sys.exit(0)
