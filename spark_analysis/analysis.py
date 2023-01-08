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
import hbase
from hbase.rest_client import HBaseRESTClient

# Logging configuration

formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# HBase configuration

client = HBaseRESTClient(['http://localhost:8080'])

VEH_STATS_PREFIXES = {
    "analysisdate" : "meta",
    "busdatasource": "meta",
    "date": "id",
    "lines": "id",
    "vehiclenumber": "id",
    "brigade": "id",
    "totaldist_km": "stats",
    "totaltime_h": "stats",
    "avgspeed_mps": "stats"
}

LINE_STATS_PREFIXES = {
    "analysisdate": "meta",
    "busdatasource": "meta",
    "date": "id",
    "lines": "id",
    "totaldistline_km": "stats",
    "totaltimeline_h": "stats",
    "avgspeedline_mps": "stats"
}

VEH_WEATHER_STATS_PREFIXES = {
    "analysisdate" : "meta",
    "busdatasource": "meta",
    "date": "id",
    "lines": "id",
    "vehiclenumber": "id",
    "brigade": "id",
    "coco": "id",
    "totaldist_km": "stats",
    "totaltime_h": "stats",
    "avgspeed_mps": "stats"
}

LINE_RAIN_STATS_PREFIXES = {
    "analysisdate" : "meta",
    "busdatasource": "meta",
    "date": "id",
    "lines": "id",
    "rain": "id",
    "totaldistline_km": "stats",
    "avgdistline_km": "stats",
    "totaltimeline_h": "stats",
    "avgtimeline_h": "stats",
    "avgspeedline_mps": "stats"
}


def fetch_by_date_hbase(table, rowkey_prefix):
    from hbase.scan_filter_helper import build_prefix_filter
    from hbase.scan import Scan

    date_filter = build_prefix_filter(rowkey_prefix)  # like "20221231"

    scan = Scan(client)
    result = scan.scan(table, date_filter)

    data = [
        {
            (cell["column"]).decode().split(":")[1]: (cell["$"]).decode()
            for cell in row["cell"]
        }
        for row in result[1]["row"]
    ]

    scan.delete_scanner()

    return data


def df_to_dict(df):
    return [
        {
            k: str(v)
            for k, v in row.asDict().items()
        }
        for row in df.collect()
    ]


def add_meta(df_dict, date_to_process, source_path):
    import datetime
    return [
        {
            **row,
            "Date": date_to_process,
            "AnalysisDate": datetime.datetime.now().isoformat(),
            "BusDataSource": source_path
        }
        for row in df_dict
    ]


def add_prefix(row, prefix_dict):
    return {
        f"{prefix_dict[k.lower()]}:{k}": v
        for k, v in row.items()
    }


def write_df_to_hbase(df_dict, table, key_function, prefix_dict):
    from hbase.put import Put
    p = Put(client)
    for row in df_dict:
        p.put(table, key_function(row), add_prefix(row, prefix_dict))


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

    logger.info("Simple statistics")
    combined.createOrReplaceTempView("Combined_sql")
    veh_stats = spark.sql("SELECT Lines, VehicleNumber, Brigade, SUM(DeltaDist_m)/1000 AS TotalDist_km, SUM(DeltaTime_s)/3600 AS TotalTime_h, AVG(NULLIF(SpeedInstant_mps,0)) AS AvgSpeed_mps FROM Combined_sql GROUP BY Lines, VehicleNumber, Brigade")

    def veh_stats_rowkey(row):
        return "{}_{}_{}_{}".format(
            row["Date"],
            row["Lines"],
            row["VehicleNumber"],
            row["Brigade"]
        )

    veh_stats_dict = add_meta(df_to_dict(veh_stats), date_to_process, source_path)
    write_df_to_hbase(veh_stats_dict, "veh_stats", veh_stats_rowkey, VEH_STATS_PREFIXES)

    veh_stats.createOrReplaceTempView("VehStats_sql")
    line_stats = spark.sql("SELECT Lines, SUM(TotalDist_km) AS TotalDistLine_km, SUM(TotalTime_h) AS TotalTimeLine_h, AVG(NULLIF(AvgSpeed_mps,0)) AS AvgSpeedLine_mps FROM VehStats_sql GROUP BY Lines")

    def line_stats_rowkey(row):
        return "{}_{}".format(
            row["Date"],
            row["Lines"]
        )

    line_stats_dict = add_meta(df_to_dict(line_stats), date_to_process, source_path)
    write_df_to_hbase(line_stats_dict, "line_stats", line_stats_rowkey, LINE_STATS_PREFIXES)

    logger.info("Rows written")

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

    logger.info("Saving to HBase...")

    def bus_weather_rowkey(row):
        return "{}_{}_{}_{}_{}".format(
            row["Date"],
            row["Lines"],
            row["coco"],
            row["VehicleNumber"],
            row["Brigade"]
        )

    bus_weather_stats_dict = add_meta(df_to_dict(bus_weather_stats), date_to_process, source_path)
    write_df_to_hbase(bus_weather_stats_dict, "bus_weather_stats", bus_weather_rowkey, VEH_WEATHER_STATS_PREFIXES)

    logger.info("Lines written")

    bus_weather_rain_stats = spark.sql("SELECT Lines, VehicleNumber, Brigade, Rain, SUM(DeltaDist_m)/1000 AS TotalDist_km, SUM(DeltaTime_s)/3600 AS TotalTime_h, AVG(NULLIF(SpeedInstant_mps,0)) AS AvgSpeed_mps FROM BusWeatherRain_sql GROUP BY Lines, VehicleNumber, Brigade, Rain")

    bus_weather_rain_stats.createOrReplaceTempView("BusWeatherStats_sql")
    bus_weather_line_stats = spark.sql("SELECT Lines, Rain, SUM(TotalDist_km) AS TotalDistLine_km, AVG(TotalDist_km) AS AvgDistLine_km, AVG(TotalTime_h) AS AvgTimeLine_h, AVG(NULLIF(AvgSpeed_mps,0)) AS AvgSpeedLine_mps FROM BusWeatherStats_sql GROUP BY Lines, Rain")

    logger.info("Saving to HBase...")

    def bus_weather_line_rowkey(row):
        return "{}_{}_{}".format(
            row["Date"],
            row["Lines"],
            row["Rain"]
        )

    bus_weather_line_stats_dict = add_meta(df_to_dict(bus_weather_line_stats), date_to_process, source_path)
    write_df_to_hbase(bus_weather_line_stats_dict, "bus_weather_line_stats", bus_weather_line_rowkey, LINE_RAIN_STATS_PREFIXES)

    logger.info("Lines written")
    logger.info("Ending application")


if __name__ == "__main__":
    if len(sys.argv) == 1:
        logger.error("This application requires a parameter [date to process]")
    date_to_process = sys.argv[1]
    main(date_to_process)
    sys.exit(0)
