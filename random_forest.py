# import findspark
from pyspark.ml.feature import CountVectorizer, IDF, Tokenizer, RegexTokenizer, StopWordsRemover, IDF, MinHashLSH
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from time import *
from pyspark.ml.evaluation import BinaryClassificationEvaluator
# from sklearn.metrics import classification_report, confusion_matrix

# findspark.init("/usr/local/Cellar/apache-spark/2.4.4/libexec")
spark = SparkSession.builder.appName("final_project").getOrCreate()
spark

CSV_PATH = "creditcard.csv"
df = spark.read.csv(CSV_PATH, inferSchema=True, header=True)
columns = [i for i in df.columns if i!='Class']

assembler = VectorAssembler(inputCols= columns, outputCol='features')
df = assembler.transform(df)

numeric_features = [t[0] for t in df.dtypes if t[1] != 'String']
df.select(numeric_features).describe().toPandas().transpose()

train, test = df.randomSplit([0.7, 0.3], seed = 2018)
print("Training Dataset Count: " + str(train.count()))
print("Test Dataset Count: " + str(test.count()))

start_time = time()

algo = RandomForestClassifier(featuresCol='features', labelCol='Class')
model = algo.fit(train)

# model.save("/Users/bharathsurianarayanan/Documents/PBDA_project")
# Saving the built model, to be used in predicting live streams at the consumer
#Loading changes to the model
model.write().overwrite().save("/Users/bharathsurianarayanan/Documents/PBDA_project/test_model")


end_time = time()
elapsed_time = end_time - start_time
print("Time to train model: %.3f seconds" % elapsed_time)

predictions = model.transform(test)
predictions.select(['Class','prediction', 'probability']).show()

evaluator = BinaryClassificationEvaluator(labelCol='Class', metricName='areaUnderROC')
evaluator.evaluate(predictions)
print("Test: Area Under ROC: " + str(evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})))

y_true = predictions.select(['Class']).collect()
y_pred = predictions.select(['prediction']).collect()
# print(classification_report(y_true, y_pred))

