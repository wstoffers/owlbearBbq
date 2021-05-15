#**    This line is 79 characters long.  The 80th character should wrap.   ***\

#imports:
import os, sys, re, json
import pyspark
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, TimestampType
from pyspark.sql.utils import AnalysisException

#define:
def interpolate(spark, window):
    print(f'herehere current directory: {os.getcwd()}')
    for resource in os.listdir(os.getcwd()):
        if window in resource:
            break
    thermaq = readBroken(resource)
    print(f'herehere read:')
    thermaq.printSchema()
    thermaq.show(20, False)
    
def readBroken(filename):
    schema = StructType([StructField('when',TimestampType()),
                         StructField(f'smokerTemp (degF)',DoubleType()),
                         StructField(f'unused',DoubleType())])
    return spark.read.csv(filename,header=True,schema=schema)

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
    print(f'{os.linesep}done!{os.linesep*5}')
