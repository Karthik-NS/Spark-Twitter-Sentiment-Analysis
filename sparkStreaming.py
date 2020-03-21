from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('twitterStreamAnalytics').getOrCreate()

from pyspark.sql.functions import lower, regexp_extract, udf

from pyspark.sql.types import StructType, StructField, StringType, DoubleType


"""Defining a function to convert Score values into rating(negative/positive/neutral) and assign a new column"""

def avg_sentiScore(Score):
	try:
		if Score < 0: return 'NEGATIVE'
		elif Score == 0: return 'NEUTRAL'
		else: return 'POSITIVE'
	except TypeError:
		return 'NEUTRAL'

# converting above function into USER DEFINED FUNCTION to work as SQL function

udf_avg_sentiScore = udf(avg_sentiScore, StringType())



dataPath = "file:///home/karthik/srikarthik/twitter/"

"""
In case of windows type absolute path of file including drive.

Example: dataPath = "C:\\filepath\\twitter\\"

Note: only specify folder path dont specify filename else it throws error in below statements

"""


# Defining Schema

dataSchema = StructType([StructField("Text",StringType(),True),StructField("Score",DoubleType(),True)])

"""
Notes:

*The method SparkSession.readStream returns a DataStreamReader used to configure the stream.

*Every streaming DataFrame must have a schema - the definition of column names and data types.
else it through error. 

*The type of stream can be: Files, Kafka, TCP/IP, console etc. we are using file as stream.
In our example below, we will be consuming files written continuously to a pre-defined directory. To control how much data is pulled into Spark at once, we can specify the option maxFilesPerTrigger.For example we will be reading in only one file for every trigger.

* We have stored tweets with fields text and score using '|' as delimiter as a text file. Hence we will read using csv format with separator as '|'.

Note Streaming data frame 

Note spark streaming do not support certain operations. You can refer https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#unsupported-operations

"""

initialDF  = spark.readStream.schema(dataSchema).option('maxTriggerPerFile',1).csv(dataPath,sep='|')

initialDF.printSchema()

"""
 by using regexp_extract function we are extraction company name like apple, microsoft, google which we have used as hashtags in twitter streaming for our analysis

Further we are adding a status column by applying udf_avg_sentiScore to do sentiment analysis

"""

streamingDF = initialDF.select(regexp_extract (lower(initialDF['Text']),'(microsoft|google|apple)',1).alias("companyName"),initialDF['Score']).withColumn("status", udf_avg_sentiScore(initialDF['Score']))


# Below command df.isStreaming allows us to check whether df is static or streaming?

streamingDF.isStreaming 


# Below we are looping over all active streams to get their id and name

for s in spark.streams.active:         
  print("{}: {}".format(s.id, s.name))


"""
The method DataFrame.writeStream returns a DataStreamWriter used to configure the output of the stream.

Please note, there are a number of parameters to the DataStreamWriter configuration:

Query's name (optional) - This name must be unique among all the currently active queries in the associated SQLContext.

Trigger (optional) - Default value is ProcessingTime(0) and it will run the query as fast as possible.

Checkpointing directory (optional)

Output mode ( complete or append or update)

Output sink: .format(file like json/csv/parquet, kafka, console, memory, etc)

Once the configuration is completed, we can trigger the job with a call to streamingdf.start()
"""


streamingQuery = streamingDF.writeStream.queryName("stream_tweet").format("memory").outputMode("append").start()


"""since we are using memory as sink which updates an in-memory table, which can be queried through Spark SQL or the DataFrame API.

Note: name given in queryName option in above statement acts like in-memory table, which can be queried 

"""

# Lets do some spark streaming analysis 

# show all values from stream_tweet 

spark.sql("SELECT * from stream_tweet").show()

# show all values where score is not null

spark.sql("SELECT * from stream_tweet where score is not null").show()


# show distinct company names 


spark.sql("SELECT DISTINCT companyName FROM stream_tweet").show()

# find total positive tweets for each company

spark.sql("SELECT companyName, count(*) as positive_count FROM stream_tweet where companyName  rlike '[a-zA-Z]' AND status = 'POSITIVE' GROUP BY companyName").show()

# find total negative tweets for each company


spark.sql("SELECT companyName, count(*) as negative_count FROM stream_tweet where companyName  rlike '[a-zA-Z]' AND status = 'NEGATIVE' GROUP BY companyName").show()

# find total neutral tweets for each company


spark.sql("SELECT companyName, count(*) as neutral_count FROM stream_tweet where companyName  rlike '[a-zA-Z]' AND status = 'NEUTRAL' GROUP BY companyName").show()


# find total count of all tweets status wise for each company

spark.sql("SELECT companyName,status, count(*) As Total_Count FROM stream_tweet where companyName  rlike '[a-zA-Z]'  GROUP BY companyName, status ORDER BY status").show()


# find the company which has max count of all tweets status wise


tempDF = spark.sql("SELECT companyName,status, count(*) As Total_Count FROM stream_tweet where companyName  rlike '[a-zA-Z]'  GROUP BY companyName, status ORDER BY status")

tempDF.createOrReplaceTempView('temp')


spark.sql("select companyName,status,Total_count as Rating from temp where total_count in (select  max(Total_count) from temp group by status) ORDER BY Total_count").show()


# Stop the stream
streamingQuery.stop()                  

# stop all remaining streams which are active by looping

for s in spark.streams.active:
  print("{}: {}".format(s.id, s.name))
  s.stop()