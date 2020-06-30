#sql functions
from pyspark.sql.types import *
from pyspark.sql import SparkSession

if __name__ == "__main__":
    sparkSession = SparkSession.builder.master('local')\
                                    .appName('SparkStreamingSQLQuery')\
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
    
    fileStreamDF.createOrReplaceTempView('disaster_accident_crime_accounts')
    
    categoryDF = SparkSession.sql("SELECT from category, following\
                                    FROM disaster_accident_crime_accounts\
                                    WHERE followers > '15000'")
    
    from pyspark.sql.functions import format_number
    from pyspark.sql.functions import col
    

    # groupby the dataframe based on category column and find the sum with aggregate functions
    recordsPerCategory = fileStreamDF.groupby('category')\
                                        .agg({'followers': 'sum'})\
                                        .withColumnRenamed('sum(followers)', 'total_followers')
                                        .orderBy('total_followers', ascending=False)\
                                        .withColumn('total_following', format_number(col('total_following'), 0))
    
    query = trimmedDF.writeStream\
                        .outputMode('complete')\
                        .format('console')\
                        .option('truncate', 'false')\
                        .option('numRows', 30)\
                        .start()\
                        .awaitTermination()