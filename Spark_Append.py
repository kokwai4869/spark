from pyspark.sql.types import *
from pyspark.sql import SparkSession

if __name__ == "__main__":
    sparkSession = SparkSession.builder.master('local')\
                                    .appName('SparkStreamingAppendMode')\
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
                                .schema(schema)\
                                .csv('../input/datasets/dropfolder')
    
    fileStreamDF = fileStreamDF.withColumnRenamed('twitter handle', 'twitter_handle')\
                                .withColumnRenamed('profile location', 'profile_location')
    
    print(' ')
    print('Is the stream ready?')
    print(fileStreamDF.isStreaming)
    
    print(' ')
    print('Schema of the input Stream')
    print(fileStreamDF.printSchema)
    
    trimmedDF = fileStreamDF.select(fileStreamDF.category,
                                   fileStreamDF.twitter_handle,
                                   fileStreamDF.profile_location,
                                   fileStreamDF.followers)\
                                    .withColumnRenamed('followers', 'companions')
    
    query = trimmedDF.writeStream\
                        .outputMode('append')\
                        .format('console')\
                        .option('truncate', 'false')\
                        .option('numRows', 30)\
                        .start()\
                        .awaitTermination()