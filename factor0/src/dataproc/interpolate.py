#**    This line is 79 characters long.  The 80th character should wrap.   ***\

#imports:
import os, sys, re, json
import pyspark
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DecimalType, TimestampType
from pyspark.sql.utils import AnalysisException

#define:
def interpolate(spark, window):
    for resource in os.listdir(os.getcwd()):
        if window in resource:
            break
    thermaq = readThermaq(resource)
    thermaq.printSchema()
    thermaq.show(20, False)
    
def readThermaq(filename):
    schema = StructType([StructField('when',TimestampType()),
                         StructField(f'smokerTemp (degF)',DecimalType(5,2)),
                         StructField(f'unused',DecimalType(5,2))])
    thermaq = spark.read.csv(filename,header=True,schema=schema)
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
