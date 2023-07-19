import sys

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import HashingTF, Tokenizer, StopWordsRemover, StringIndexer
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

BEARER_TOKEN = ""

if __name__ == "__main__":

    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    topics = sys.argv[3]
    
    spark = SparkSession.builder.appName("spark test").getOrCreate()
    
    tweets_s3_data = spark.read.option("header","true").option("inferschema","true").csv(sys.argv[4])
    tweets_data = tweets_s3_data.filter(tweets_s3_data.text.isNotNull())
    train, test = tweets_data.randomSplit([0.95, 0.05], seed=12345)
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    # stop word remover
    stop_words_remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="noStopWords")
    # term hashing
    hashing_TF = HashingTF(inputCol=stop_words_remover.getOutputCol(), outputCol="features")
    # label conversion
    indexer = StringIndexer(inputCol="airline_sentiment", outputCol="label")
    # logistic regression
    lr = LogisticRegression(maxIter=5,labelCol='label',featuresCol='features')
    # pipeline creation 
    pipeline = Pipeline(stages=[tokenizer,stop_words_remover, hashing_TF, indexer, lr])
    # hyper params tuning
    paramGrid = ParamGridBuilder()     .addGrid(hashing_TF.numFeatures, [10, 100, 1000])     .addGrid(lr.regParam, [0.1, 0.01])     .build()
    evaluator = MulticlassClassificationEvaluator(labelCol ="label", predictionCol ="prediction")
    #cross validation
    crossvalidator = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=evaluator,
                          numFolds=2) 
    
    # model creation using training data 
    cvModel = crossvalidator.fit(train)
    test = spark            .readStream            .format("kafka")            .option("kafka.bootstrap.servers", bootstrapServers) .option("failOnDataLoss", "false")           .option(subscribeType, topics)            .load()
    test = test.selectExpr("CAST(value AS STRING)")
    test = test.toDF("text")

    # scores on the test set
    result = cvModel.transform(test)
    # print(result.take(10))
    print(result.isStreaming)
    
    result = result.select(to_json(struct("prediction")).alias("value"), to_json(struct("features")).alias("key"))

    query = result.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").writeStream      .format("kafka")      .option("kafka.bootstrap.servers", "localhost:9092")      .option("path", "./parc")      .option("checkpointLocation", "./zd")      .option("topic", "olympics_sentiment")      .start()

    query.awaitTermination()
