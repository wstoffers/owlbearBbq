#**    This line is 79 characters long.  The 80th character should wrap.   ***\

#imports:
import os, sys, re, json
import pyspark
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, ArrayType
from pyspark.sql.types import DecimalType, TimestampType, ByteType, IntegerType
from pyspark.sql.types import MapType, StringType, FloatType, ShortType
from pyspark.sql.functions import lit, to_timestamp
from pyspark.sql.utils import AnalysisException

#define:
combineQuery = '''
    --combine/deduplicate as stopgap for thermocouple splitting/duplicates issue
    (SELECT
         when,
         smokerTempDegF,
         unused
     FROM
         start)
    UNION
    (SELECT
         when,
         smokerTempDegF,
         unused
     FROM
         end);
'''

interpolateQuery = '''
    --Spark SQL doesn't support CREATE FUNCTION without a *.jar yet
    WITH apiWeather AS(
        --Convert from Kelvin to Fahrenheit:
        SELECT 
            owlbear.when AS when,
            FORMAT_NUMBER(
                (owlbear.temperature.temp - 273.15) * 9/5 + 32, 2
            ) AS owlbearTempDegF,
            FORMAT_NUMBER(
                (franklin.temperature.temp - 273.15) * 9/5 + 32, 2
            ) AS franklinTempDegF
        FROM
            owlbear
        INNER JOIN franklin ON
            owlbear.when = franklin.when
    ), combinedWhen AS(
        --Partition thermaq, the larger/dense table
        --Broadcast apiWeather, the smaller/sparse table
        SELECT --/*+ BROADCAST(apiWeather) */
            CASE WHEN thermaq.when IS NULL
                THEN apiWeather.when 
                ELSE thermaq.when    
                END AS when,
            apiWeather.when AS apiWhen, 
            thermaq.smokerTempDegF,     
            apiWeather.owlbearTempDegF,
            apiWeather.franklinTempDegF
        FROM
            thermaq
        FULL OUTER JOIN apiWeather ON
            thermaq.when = apiWeather.when
        ORDER BY
            when
    ), calcPrep AS(
        --Prepare for interpolation between last and next non null values:
        SELECT
            when,
            smokerTempDegF AS smokerTemp,
            UNIX_TIMESTAMP(when) AS x,
            UNIX_TIMESTAMP(
                LAST(apiWhen, TRUE) OVER(
                    lookback
                )
            ) AS x0,
            UNIX_TIMESTAMP(
                FIRST(apiWhen, TRUE) OVER(
                    lookahead
                )
            ) AS x1,
            LAST(owlbearTempDegF, TRUE) OVER(
                lookback
            ) AS owlbearY0,
            FIRST(owlbearTempDegF, TRUE) OVER(
                lookahead
            ) AS owlbearY1,
            LAST(franklinTempDegF, TRUE) OVER(
                lookback
            ) AS franklinY0,
            FIRST(franklinTempDegF, TRUE) OVER(
                lookahead
            ) AS franklinY1
        FROM
            combinedWhen
        WINDOW
            lookback AS (ORDER BY when 
                RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ), lookahead AS (ORDER BY when
                RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
            )
    )
    --Perform interpolation for sparse data:
    SELECT
        when,
        smokerTemp,
        ROUND(
            owlbearY0 + (x-x0) * ((owlbearY1-owlbearY0)/(x1-x0)), 2
        ) AS owlbearAPI,
        ROUND(
            franklinY0 + (x-x0) * ((franklinY1-franklinY0)/(x1-x0)), 2
        ) AS franklinAPI
    FROM
        calcPrep
    WHERE
        smokerTemp IS NOT NULL;
'''

def interpolate(spark, window):
    '''Read data into DataFrames, prepare/execute SQL query

    Args:
        spark: Spark Session object
        window: String holding date/time range of thermocouple data to read

    '''
    thermaq = readThermaq(spark, window)
    thermaq.printSchema()
    thermaq.show(10, False)
    owlbearApi = readJson(spark, 'owlbear', window)
    franklinApi = readJson(spark, 'franklin', window)
    owlbearApi.show(truncate=False)
    franklinApi.show(truncate=False)
    thermaq.createOrReplaceTempView("thermaq")
    franklinApi.createOrReplaceTempView("franklin")
    owlbearApi.createOrReplaceTempView("owlbear")
    result = spark.sql(interpolateQuery)
    result.explain()
    result.show(10,truncate=False)

def readJson(spark, prefix, timebox):
    '''Find and read multiline JSON files that fit within date/time range

    Args:
        spark: Spark Session object
        prefix: String restaurant name 'owlbear' or 'franklin'
        timebox: String holding date/time range of thermocouple data

    Returns:
        DataFrame of API weather data from JSON files matching date/time range

    '''
    whenFormat = '%Y.%m.%d.%H.%M.%S'
    start, end = (datetime.strptime(dt,
                                    whenFormat) for dt in timebox.split('-'))
    start -= timedelta(minutes=1)
    end += timedelta(minutes=1)
    regex = re.compile(f'^{prefix}.+(202[0-9]-[0-9][0-9]-[0-9][0-9]-'
                       f'.+)-.+json$')
    apiSnapshot = False
    for resource in os.listdir(os.getcwd()):
        match = regex.match(resource)
        if match:
            when = datetime.strptime(match.group(1),'%Y-%m-%d-%H.%M.%S.%f')
            if start < when < end:
                df = (spark.read.option("multiLine", True)
                                .schema(jsonSchema())
                                .json(resource)
                      )
                df = df.withColumn('when',
                                   to_timestamp(lit(when.timestamp())))
                try:
                    apiSnapshot = apiSnapshot.union(df)
                except AttributeError:
                    apiSnapshot = df
    return apiSnapshot

def jsonSchema():
    return (StructType().add('clouds',ByteType())
                        .add('humidity',ByteType())
                        .add('rain',MapType(StringType(),FloatType()))
                        .add('snow',MapType(StringType(),FloatType()))
                        .add('wind',MapType(StringType(),FloatType()))
                        .add('pressure',StructType().add('press',ShortType()))
                        .add('temperature',StructType().add('temp',
                                                            DecimalType(5,2)))
                        .add('visibility_distance',IntegerType())
            )

def readThermaq(spark, window):
    '''Read thermocouple CSV(s) that match(es) date/time range

    Intended to read a single CSV that holds date/time range but expanded to 
        allow for two separate CSVs to cover date/time range as a workaround
        for thermocouple splitting/duplicates issue

    Args:
        spark: Spark Session object
        timebox: String holding date/time range of thermocouple data to read

    Returns:
        DataFrame of thermocouple temperature data

    '''
    start, end = window.split('-')
    for resource in os.listdir(os.getcwd()):
        if start in resource:
            startCsv = resource
        if end in resource:
            endCsv = resource
    schema = StructType([StructField('when',TimestampType()),
                         StructField(f'smokerTempDegF',DecimalType(5,2)),
                         StructField(f'unused',DecimalType(5,2))])
    if startCsv == endCsv:
        thermaq = spark.read.csv(startCsv,header=True,schema=schema)
    else:
        start = spark.read.csv(startCsv,header=True,schema=schema)
        start.createOrReplaceTempView('start')
        end = spark.read.csv(endCsv,header=True,schema=schema)
        end.createOrReplaceTempView('end')
        thermaq = spark.sql(combineQuery)
    return thermaq.drop('unused')

#run:
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=f'calculates matching values '
                                                 f'for the date/time range '
                                                 f'provided, using API and '
                                                 f'thermocouple data files '
                                                 f'in the current working '
                                                 f'directory')
    parser.add_argument('--range', '-r', required=True,
                        help='date/time range of thermocouple data')
    args = parser.parse_args()
    spark = (pyspark.sql.SparkSession.builder
                                     .master('local[*]')
                                     .appName('Initial Interpolation, Local')
                                     .getOrCreate()
             )
    sc = spark.sparkContext
    interpolate(spark, args.range)
    sc.stop()
