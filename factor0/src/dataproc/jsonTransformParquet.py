#**    This line is 79 characters long.  The 80th character should wrap.   ***\

#imports:
import os, sys, re, json
#import socket
import pyspark
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import IntegerType, StringType, FloatType
from pyspark.sql.utils import AnalysisException
#from pyspark.sql import Row
from google.cloud import storage

#define:
fileQuery = '''SELECT 
                   datetime,
                   day,
                   MAX(location) as locationone,
                   MAX(filename) as fileone,
                   MIN(location) as locationtwo,
                   MIN(filename) as filetwo
               FROM json_files 
               GROUP BY 
                   datetime,
                   day
               HAVING
                   COUNT(1) > 1;'''

def getJsons():
    client = storage.Client()
    bucket = client.get_bucket('wstoffers-galvanize-owlbear-data-lake-raw')
    for json in client.list_blobs(bucket):
        print(f'Checking {json.name}')
        localData = os.path.join(os.getcwd(),'localData')
        if json.name not in os.listdir(localData):
            filePath = os.path.join(localData,json.name)
            bucket.get_blob(json.name).download_to_filename(filePath)

class sousOrganizer(object):
    """Organization assistant that handles I/O for an individual record

    Reads JSON data, joins weather from both locations into a single record
        based on API call time, writes Parquet data.

    Args:
        spark: Session object from PySpark
        row: Row object containing data to spread into attributes

    Attributes:
        datetime: String representing date and time of API call
        day: String representing day of the week abbreviation
        owlbear: Dynamically set by row.locationone or row.locationtwo, 
            containing a string representation of the JSON file path
        franklin: Dynamically set by row.locationone or row.locationtwo, 
            containing a string representation of the JSON file path
        owlbearDf: Data read in from JSON file
        franklinDf: Data read in from JSON file
        spark: Session object from PySpark

    """

    def __init__(self,spark,row):
        self.datetime = row.datetime
        self.day = row.day
        setattr(self,row.locationone,row.fileone)
        setattr(self,row.locationtwo,row.filetwo)
        self.spark = spark
        parquetFile = f'{self.datetime}.parquet'
        try:
            #read in for compaction:
            self.combineDf = spark.read.parquet(parquetFile)
        except AnalysisException:
            #assume pyspark.sql.utils.AnalysisException: Path does not exist:
            with open(os.path.join(os.getcwd(),
                                   'localData',
                                   self.owlbear)) as jsonFile:
                decoded = json.load(jsonFile)
            self.owlbearDf = self._manualDf(decoded, 'owlbear')
            #repeated code should be broken out:
            with open(os.path.join(os.getcwd(),
                                   'localData',
                                   self.franklin)) as jsonFile:
                decoded = json.load(jsonFile)
            self.franklinDf = self._manualDf(decoded, 'franklin')
            self.combinedDf = self.owlbearDf.join(self.franklinDf,('refTime'))
            self.combinedDf.write.parquet(parquetFile)
        #work around java heap space error:
        #default pyspark expects jsonl format not multiline json:
        #self.owlbearDf = (spark.read.option('multiline','true')
        #                            .json(os.path.join(os.getcwd(),
        #                                               'localData',
        #                                               self.owlbear))
        #                  )
        #self.franklinDf = (spark.read.option('multiline','true')
        #                             .json(os.path.join(os.getcwd(),
        #                                                'localData',
        #                                                self.franklin))
        #                   )
    def _manualDf(self, json, name):
        name = name.title()
        schema = StructType([StructField('refTime',IntegerType(),False),
                             StructField(f'clouds{name}',IntegerType(),True),
                             StructField(f'rain{name}',FloatType(),True),
                             StructField(f'snow{name}',FloatType(),True),
                             StructField(f'windspeed{name}',FloatType(),True),
                             StructField(f'winddir{name}',IntegerType(),True),
                             StructField(f'humidity{name}',IntegerType(),True),
                             StructField(f'pressure{name}',IntegerType(),True),
                             StructField(f'temp{name}',FloatType(),True),
                             StructField(f'feels{name}',FloatType(),True),
                             StructField(f'visible{name}',IntegerType(),True),
                             StructField(f'status{name}',StringType(),True)])
        row = (int(json['reference_time']),
               int(json['clouds']),
               None if not json['rain'] else float(json['rain']['1h']),
               None if not json['snow'] else float(json['snow']['1h']),
               float(json['wind']['speed']),
               int(json['wind']['deg']),
               int(json['humidity']),
               int(json['pressure']['press']),
               float(json['temperature']['temp']),
               float(json['temperature']['feels_like']),
               int(json['visibility_distance']),
               json['status'])
        #for key in decodedJson:
        #    if key not in keep:
        #        newlyKeyed[key+name.title()] = decodedJson[key]
        #row = Row(**newlyKeyed)
        #return self.spark.createDataFrame(row)
        return self.spark.createDataFrame([row],schema)
            
class organizer(object):
    """Organizer to simplify working with local data

    Finds JSON data files inside localData/ and pairs them by Open Weather Map
        API call time.

    Args:
        spark: Session object from PySpark

    Attributes:
        directory: Path to local data
        contents: List of directory contents
        spark: Session object from PySpark
        pairs: List of helper objects that hold corresponding data pairs

    """
    
    def __init__(self, spark):
        #this is repeated and should be refactored:
        self.directory = os.path.join(os.getcwd(),'localData')
        self.contents = os.listdir(self.directory)
        self.spark = spark
        self.pairs = self._group()

    def _group(self):
        extracted = []
        condition = re.compile('(^[a-z]+)(.*)-([A-Za-z]+)\.json$')
        for filename in self.contents:
            matches = condition.match(filename)
            if matches:
                extracted.append([matches.group(2),
                                  matches.group(1),
                                  matches.group(3),
                                  matches.group(0)])
        return self._pair(extracted)
        
    def _pair(self,rows):
        columns = ['datetime','location','day','filename']
        dataframe = self.spark.createDataFrame(rows,columns)
        dataframe.createOrReplaceTempView("json_files")
        result = self.spark.sql(fileQuery)
        #map can't use argument to execute function that doesn't alter data?
        #inefficient workaround/dangerous for memory if large data:
        return [sousOrganizer(self.spark,r) for r in result.collect()]

#run:
if __name__ == '__main__':
    #print(f'{os.linesep*2}socket.gethostname()')
    #sc = pyspark.SparkContext()
    #getJsons() #bad idea even with 13k-14k files?
    
    spark = (pyspark.sql.SparkSession.builder
                                     .master('local[*]')
                                     .appName('Initial Transform, Local')
                                     .getOrCreate()
             )
    sc = spark.sparkContext
    print(f'{os.linesep*2}Spark Version: {sc.version}')
    print(f'sc.master: {sc.master}, parallelism: {sc.defaultParallelism}')
    print(f'{os.linesep*2}')
    organizer(spark)
    
    sc.stop()
    print(f'{os.linesep}done!{os.linesep*5}')
