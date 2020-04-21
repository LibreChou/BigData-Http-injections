# import findspark
# findspark.init('/usr/local/spark/spark-2.4.0-bin-hadoop2.7/')
import pyspark
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType
from pyspark.sql.functions import udf
from pyspark.ml.feature import CountVectorizerModel, IDFModel, StandardScalerModel, Tokenizer
from pyspark.ml.classification import LogisticRegressionModel


from urllib.parse import unquote

def to_ngram(payload_obj):
    n=2
    payload = str(payload_obj)
    ngrams = ''
    for i in range(0,len(payload)-n + 1):
        ngrams += payload[i:i+n]+ ' '
    return ngrams[:-1]


# define a function to compute sentiments of the received tweets
def get_prediction(queries):
    try:
        queries = queries.map(lambda w: Row(payload=w))
        queries = sqlc.createDataFrame(queries)

        queries = queries.withColumn('ngrams', ngrams(queries['payload']))
        queries = tokenizer.transform(queries)
        queries = vectorizer.transform(queries)
        queries = idf_model.transform(queries)
        queries = scalerModel.transform(queries)
        preds = model.transform(queries)
        preds.select('payload','prediction').show()

    except : 
        print('No data')

APP_NAME = "BigData"
conf = pyspark.SparkConf().setAll([ ('spark.app.name', APP_NAME),('spark.executor.memory', '8g'), ('spark.cores.max', '2'), ('spark.driver.memory','8g')])
sc = SparkContext(conf=conf)
sqlc = SQLContext(sc)
    
ngrams = udf(to_ngram, StringType())
tokenizer = Tokenizer.load('models/Tokenizer')
vectorizer = CountVectorizerModel.load('models/Vectorizer')
idf_model = IDFModel.load('models/idf')
scalerModel = StandardScalerModel.load('models/scalerModel')
model = LogisticRegressionModel.load('models/Logistic_Regression_Model')
ssc = StreamingContext(sc, batchDuration= 3)
lines = ssc.socketTextStream("localhost", 9999)

lines.foreachRDD(get_prediction)

ssc.start()             
ssc.awaitTermination()
