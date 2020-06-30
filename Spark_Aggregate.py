#Aggregate functions
from pyspark.sql.types import *
from pyspark.sql import SparkSession

if __name__ == "__main__":
    sparkSession = SparkSession.builder.master('local')\
                                    .appName('SparkStreamingAggregate')\
                                    .getOrCreate()
    
    sparkSession.sparkContext.setLogLevel('ERROR')
    
    schema = StructType([StructField('category', StringType(), True),
                         StructField('on twitter since', StringType(), True),
                         StructField('twitter handle', StringType(), True),
                         StructField('profile url', StringType(), True),
                         StructField('followers', StringType(), True),
                         StructField('following', StringType(), True),
                         StructField('profile location', StringType(), True),
                         StructField('profile lat/lon', StringType(), True),
                         StructField('profile description', StringType(), True)
    ])
    
    fileStreamDF = sparkSession.readStream\
                                .option('header', 'true')\
                                .option('maxFilesPerTrigger', 1)\
                                .schema(schema)\
                                .csv('../input/datasets/dropfolder')

    # groupby the dataframe based on category column and find the sum with aggregate functions
    recordsPerCategory = fileStreamDF.groupby('category')\
                                        .agg({'followers': 'sum'})\
                                        .withColumnRenamed('sum(followers)', 'total_followers')
                                        .orderBy('total_followers', ascending=False)
    
    query = trimmedDF.writeStream\
                        .outputMode('complete')\
                        .format('console')\
                        .option('truncate', 'false')\
                        .option('numRows', 30)\
                        .start()\
                        .awaitTermination()