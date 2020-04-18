#!/usr/bin/env python
# coding: utf-8

# In[6]:


# import findspark
# findspark.init("/usr/local/Cellar/apache-spark/2.4.4/libexec")

from pyspark.ml.feature import CountVectorizer, IDF, Tokenizer, RegexTokenizer, StopWordsRemover, IDF, MinHashLSH

from pyspark.sql import SparkSession
from pyspark import SparkContext
spark = SparkSession        .builder        .appName("final_project")        .getOrCreate()

spark


# In[55]:


CSV_PATH = "creditcard.csv"
APP_NAME = "Random Forest Example"
SPARK_URL = "local[*]"
RANDOM_SEED = 13579
RF_NUM_TREES = 3
RF_MAX_DEPTH = 4
RF_NUM_BINS = 32


# In[63]:


df = spark.read     .options(header = "true", inferschema = "true")     .csv(CSV_PATH)

print("Total number of rows: %d" % df.count())
len(df.columns)


# In[65]:


from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

transformed_df = df.rdd.map(lambda row: LabeledPoint(row[-1], Vectors.dense(row[0:-1])))


training_data, test_data = transformed_df.randomSplit([0.7,0.3], 0)

print("Number of training set rows: %d" % training_data.count())
print("Number of test set rows: %d" % test_data.count())


# In[68]:


from pyspark.mllib.tree import RandomForest
from time import *

start_time = time()

model = RandomForest.trainClassifier(training_data, numClasses=2, categoricalFeaturesInfo={},     numTrees=RF_NUM_TREES, featureSubsetStrategy="auto", impurity="gini",     maxDepth=RF_MAX_DEPTH, seed=0)

end_time = time()
elapsed_time = end_time - start_time
print("Time to train model: %.3f seconds" % elapsed_time)


# In[70]:


predictions = model.predict(test_data.map(lambda x: x.features))
print(predictions)
labels_and_predictions = test_data.map(lambda x: x.label).zip(predictions)
acc = labels_and_predictions.filter(lambda x: x[0] == x[1]).count() / float(test_data.count())
print("Model accuracy: %.3f%%" % (acc * 100))


# In[73]:


f = open('debug.txt', 'w')
f.write(model.toDebugString())


# In[81]:


# from pyspark.mllib.evaluation import MulticlassMetrics

# predictionAndLabels = test_data.map(lambda lp: (float(model.predict(lp.features)), lp.label))
# metrics = MulticlassMetrics(predictionAndLabels)


# In[ ]:




