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
interpolateQuery = '''
    --spark sql doesn't support CREATE FUNCTION without a *.jar yet
    --for maintainability, this all-sql syntax was refactored with scala udfs
    WITH apiWeather AS(
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
    )
    SELECT /*+ BROADCAST(apiWeather) */
        thermaq.when,
        thermaq.smokerTempDegF,
        apiWeather.owlbearTempDegF,
        apiWeather.franklinTempDegF
    FROM
        thermaq
    LEFT JOIN apiWeather ON
        thermaq.when = apiWeather.when;
'''

def interpolate(spark, window):
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
    for resource in os.listdir(os.getcwd()):
        if window in resource:
            break    
    schema = StructType([StructField('when',TimestampType()),
                         StructField(f'smokerTempDegF',DecimalType(5,2)),
                         StructField(f'unused',DecimalType(5,2))])
    thermaq = spark.read.csv(resource,header=True,schema=schema)
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
