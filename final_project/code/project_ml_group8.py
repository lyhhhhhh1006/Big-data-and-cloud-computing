# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit, count, isnan, when
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import json
from pyspark.ml import Pipeline
from sparknlp.annotator import *
from sparknlp.base import *
import sparknlp
from sparknlp.pretrained import PretrainedPipeline

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Data

# COMMAND ----------

comments_final = spark.read.parquet("/Dataset/comments_final")
submissions_final = spark.read.parquet("/Dataset/submissions_final")

# COMMAND ----------

## Extract subreddits from submissions
datingapp_df_submission = submissions_final.filter((col("subreddit")=='Bumble') | (col("subreddit")=='Tinder')|(col("subreddit")=='hingeapp')|(col("subreddit")=='dating')| (col("subreddit")=='DatingApps')).cache()

relationship_df_submission = submissions_final.filter((col("subreddit")=='relationships') | (col("subreddit")=='AskMen') | (col("subreddit")=='AskWomen')).cache()

advice_df_submission = submissions_final.filter((col("subreddit")=='relationship_advice') | (col("subreddit")=='datingoverforty')|(col("subreddit")=='datingoverthirty')| (col("subreddit")=='datingadvice')| (col("subreddit")=='datingoverforty')| (col("subreddit")=='datingoverfifty')).cache()

## Extract subreddits from comments
datingapp_df_comments = comments_final.filter((col("comment_subreddit")=='Bumble') | (col("comment_subreddit")=='Tinder')|(col("comment_subreddit")=='hingeapp')|(col("comment_subreddit")=='dating')| (col("comment_subreddit")=='DatingApps')).cache()

relationship_df_comments = comments_final.filter((col("comment_subreddit")=='relationships') | (col("comment_subreddit")=='AskMen') | (col("comment_subreddit")=='AskWomen')).cache()

advice_df_comments = comments_final.filter((col("comment_subreddit")=='relationship_advice') | (col("comment_subreddit")=='datingoverforty')|(col("comment_subreddit")=='datingoverthirty')| (col("comment_subreddit")=='datingadvice')| (col("comment_subreddit")=='datingoverforty')| (col("comment_subreddit")=='datingoverfifty')).cache()

# COMMAND ----------

comments_final.printSchema()

# COMMAND ----------

submissions_final.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Preparing data

# COMMAND ----------

# MAGIC %md
# MAGIC Comments:  
# MAGIC y: can_gild  
# MAGIC x: Day of the week, hour, topic, sentiment, comment_score, controversiality, stickied, text_length  
# MAGIC   
# MAGIC   
# MAGIC Submission:   
# MAGIC y: over_18  
# MAGIC x: Day of the week, hour, topic, sentiment, num_comments, score, stickied，text_length  

# COMMAND ----------

#Check the distribution of selected variables.
import pyspark.sql.functions as f

submissions_final.groupBy('over_18').agg(f.count('score').alias('count')).show()

# COMMAND ----------

submissions_final.groupBy('stickied').agg(f.count('score').alias('count')).show()

# COMMAND ----------

comments_final.groupBy('stickied').agg(f.count('comment_score').alias('count')).show()

# COMMAND ----------

comments_final.groupBy('gilded').agg(f.count('comment_score').alias('count'),
         f.mean('comment_score').alias('mean'),
         f.stddev('comment_score').alias('std'),
         f.min('comment_score').alias('min'),
         f.expr('percentile(comment_score, array(0.25))')[0].alias('%25'),
         f.expr('percentile(comment_score, array(0.5))')[0].alias('%50'),
         f.expr('percentile(comment_score, array(0.75))')[0].alias('%75'),
         f.max('comment_score').alias('max')).show()

# COMMAND ----------

comments_final.groupBy('can_gild').agg(f.count('comment_score').alias('count')).show()

# COMMAND ----------

import pyspark.sql.functions as f
## Add topic
# label : dating_app, relationship, advice
datingapp_df_submission = datingapp_df_submission.withColumn("submission_topic", f.lit('datingapp'))
#datingapp_df_submission.show()
relationship_df_submission = relationship_df_submission.withColumn("submission_topic", f.lit('relationship'))
#relationship_df_submission.show()
advice_df_submission = advice_df_submission.withColumn("submission_topic", f.lit('advice'))
#advice_df_submission.show()

datingapp_df_comments = datingapp_df_comments.withColumn("comments_topic", f.lit('datingapp'))
#datingapp_df_comments.show()
relationship_df_comments = relationship_df_comments.withColumn("comments_topic", f.lit('relationship'))
#relationship_df_comments.show()
advice_df_comments = advice_df_comments.withColumn("comments_topic", f.lit('advice'))
#advice_df_comments.show()


# COMMAND ----------

submission_union = datingapp_df_submission.unionAll(relationship_df_submission).unionAll(advice_df_submission)
comments_union = datingapp_df_comments.unionAll(relationship_df_comments).unionAll(advice_df_comments)

# COMMAND ----------

## text length
import pyspark.sql.functions as f

#comments_union=comments_union.filter((comments_union.body != '[deleted]' )& (comments_union.body != '[removed]'))
#submission_union = submission_union.filter((submission_union.selftext != '[deleted]' )& (submission_union.selftext != '[removed]'))

comments_union=comments_union.withColumn("length", f.length(col("body")))

submission_union=submission_union.withColumn("length", f.length(col("title")))


# COMMAND ----------

submission_union.show()
comments_union.show()

# COMMAND ----------

## Add Sentiment
documentAssembler = DocumentAssembler()\
    .setInputCol("body")\
    .setOutputCol("document")
    
use = UniversalSentenceEncoder.pretrained(name="tfhub_use", lang="en")\
 .setInputCols(["document"])\
 .setOutputCol("sentence_embeddings")


sentimentdl = SentimentDLModel.pretrained(name='sentimentdl_use_imdb', lang="en")\
    .setInputCols(["sentence_embeddings"])\
    .setOutputCol("sentiment")

nlpPipeline = Pipeline(
      stages = [
          documentAssembler,
          use,
          sentimentdl
      ])

# COMMAND ----------

comments_union_ml = nlpPipeline.fit(comments_union).transform(comments_union)
comments_union_ml.cache

# COMMAND ----------

comments_union_ml = comments_union_ml.withColumn('sentiment', col('sentiment.result')[0]).drop('sentence_embeddings').drop('document')
comments_union_ml.printSchema()

# COMMAND ----------

comments_union_ml.show()

# COMMAND ----------

documentAssembler = DocumentAssembler()\
    .setInputCol("title")\
    .setOutputCol("document")
    
use = UniversalSentenceEncoder.pretrained(name="tfhub_use", lang="en")\
 .setInputCols(["document"])\
 .setOutputCol("sentence_embeddings")


sentimentdl = SentimentDLModel.pretrained(name='sentimentdl_use_imdb', lang="en")\
    .setInputCols(["sentence_embeddings"])\
    .setOutputCol("sentiment")

nlpPipeline1 = Pipeline(
      stages = [
          documentAssembler,
          use,
          sentimentdl
      ])

# COMMAND ----------

submission_union_ml = nlpPipeline1.fit(submission_union).transform(submission_union)
submission_union_ml.cache

# COMMAND ----------

submission_union_ml = submission_union_ml.withColumn('sentiment', col('sentiment.result')[0]).drop('sentence_embeddings').drop('document')
submission_union_ml.printSchema()

# COMMAND ----------

submission_union_ml.groupBy('sentiment').agg(f.count('score').alias('count')).show()

# COMMAND ----------

##Add timestamp
from pyspark.sql.functions import *
# Step 1: transform to the correct col format
comments_union_ml = comments_union_ml.withColumn("timestamp", from_unixtime("comment_created_utc", "yyyy-MM-dd HH:mm:ss"))

# Step 2 & 3: Extract the needed information
comments_union_ml = comments_union_ml.withColumn('Day_of_week', dayofweek(comments_union_ml.timestamp))
comments_union_ml = comments_union_ml.withColumn('Hour', hour(comments_union_ml.timestamp))

# COMMAND ----------

# Step 1: transform to the correct col format
submission_union_ml = submission_union_ml.withColumn("timestamp", from_unixtime("created_utc", "yyyy-MM-dd HH:mm:ss"))

# Step 2 & 3: Extract the needed information
submission_union_ml = submission_union_ml.withColumn('Day_of_week', dayofweek(submission_union_ml.timestamp))
submission_union_ml = submission_union_ml.withColumn('Hour', hour(submission_union_ml.timestamp))

# COMMAND ----------

comments_union_ml.show()

# COMMAND ----------

submission_union_ml.show()

# COMMAND ----------

submission_union_ml.groupby("Day_of_week").count().show()

# COMMAND ----------

submission_union_ml.groupby("Hour").count().show()

# COMMAND ----------

comments_union_ml.printSchema()

# COMMAND ----------

##Select Columns

comments_ML = comments_union_ml.select(col("can_gild"),col("Day_of_week"),col("Hour"),col("comments_topic"),col("sentiment"),col("comment_score"),col("controversiality"),col("stickied"),col("length"))
comments_ML.printSchema()

submission_ML = submission_union_ml.select(col("over_18"),col("Day_of_week"),col("Hour"),col("submission_topic"),col("sentiment"),col("num_comments"),col("score"),col("stickied"),col("length"))
submission_ML.printSchema()

# COMMAND ----------

comments_ML.printSchema()

# COMMAND ----------

submission_ML.printSchema()

# COMMAND ----------

print("Comments_ML Dataframe has ", comments_ML.count(), "Rows",  len(comments_ML.columns), "Columns")
print("submission_ML Dataframe has ", submission_ML.count(), "Rows",  len(submission_ML.columns), "Columns")

# COMMAND ----------

for col in submission_ML.columns:
    print(col, "with null values: ", submission_ML.filter(submission_ML[col].isNull()).count())

# COMMAND ----------

from pyspark.sql.types import IntegerType
comments_ML=comments_ML.withColumn("stickied",col("stickied").cast(IntegerType()))
comments_ML=comments_ML.withColumn("can_gild",col("can_gild").cast(IntegerType()))
submission_ML=submission_ML.withColumn("stickied",col("stickied").cast(IntegerType()))
submission_ML=submission_ML.withColumn("over_18",col("over_18").cast(IntegerType()))


# COMMAND ----------

comments_ML.printSchema()

# COMMAND ----------

submission_ML.printSchema()

# COMMAND ----------

comments_ML_small = comments_ML.sample(True, 0.0001, 1234)
comments_padf = comments_ML_small.toPandas()
comments_padf.to_csv('comments.csv')


# COMMAND ----------

# MAGIC %md
# MAGIC # Analyze Comments Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Split data into train, test, and split

# COMMAND ----------

# create small dataset
from random import sample
comments_small = comments_ML.sample(True, 0.0001, 1234)

# COMMAND ----------

#train_data, test_data, predict_data = comments_ML.randomSplit([0.8, 0.18, 0.02], 24)
train_data, test_data, predict_data = comments_small.randomSplit([0.8, 0.18, 0.02], 24)

# COMMAND ----------

train_data.show()

# COMMAND ----------

#print("Number of training records: " + str(train_data.count()))
#print("Number of testing records : " + str(test_data.count()))
#print("Number of prediction records : " + str(predict_data.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create pipeline and train random forest models to classify whether the comments are gild.

# COMMAND ----------

from pyspark.ml.feature import OneHotEncoder, StringIndexer, IndexToString, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline, Model

# COMMAND ----------

# Convert all the string fields to numeric indices by StringIndexer estimator.
stringIndexer_topic = StringIndexer(inputCol="comments_topic", outputCol="topic_ix", handleInvalid="keep")
stringIndexer_sentiment = StringIndexer(inputCol="sentiment", outputCol="sentiment_ix", handleInvalid="keep")


# COMMAND ----------

# Convert the index variables that have more than two levels by the function OneHotEncoder.
onehot_topic = OneHotEncoder(inputCol="topic_ix", outputCol="topic_vec")
onehot_sentiment = OneHotEncoder(inputCol="sentiment_ix", outputCol="sentiment_vec")
onehot_day_of_week = OneHotEncoder(inputCol="Day_of_week", outputCol="day_of_week_vec")
onehot_hour = OneHotEncoder(inputCol="Hour", outputCol="hour_vec")

# COMMAND ----------

# Create a feature vector by combining all features together using the vectorAssembler method

vectorAssembler_features = VectorAssembler(inputCols=["Day_of_week","Hour","topic_vec","sentiment_vec","comment_score","controversiality","stickied","length"], 
#    outputCol= "features")
#     inputCols=["topic_vec","sentiment_vec","comment_score","controversiality","stickied","length_vec"], 
#     outputCol= "features")
    inputCols=["topic_vec","sentiment_vec","comment_score","controversiality","stickied","length","hour_vec","day_of_week_vec"], 
    outputCol= "features")

# COMMAND ----------

#Build the random forest model for "author_premium" for classification using the RandomForestClassifier estimator. We build two models with numTrees = 200 and 300 respectively.

rf1 = RandomForestClassifier(labelCol="gild_label", featuresCol="features", numTrees=200, maxDepth=8)

# COMMAND ----------

# Build the pipeline that consists of transformers and an estimator.
pipeline_rf = Pipeline(stages=[stringIndexer_topic, 
                               stringIndexer_sentiment,
                               #label_stringIdx,
                               onehot_topic,
                               onehot_sentiment,
                               onehot_length,
                               vectorAssembler_features, 
                               rf1])

# COMMAND ----------

model_rf = pipeline_rf.fit(train_data)

# COMMAND ----------

model_rf.save("/FileStore/my_folder/fitted_models/rf_comments")

# COMMAND ----------

rf2 = RandomForestClassifier(labelCol="can_gild", featuresCol="features", numTrees=300, maxDepth=8)

# COMMAND ----------

pipeline_rf2 = Pipeline(stages=[stringIndexer_topic, 
                               stringIndexer_sentiment,
                               #label_stringIdx,
                               onehot_topic,
                               onehot_sentiment,
                               onehot_length,
                               vectorAssembler_features, 
                               rf2])

# COMMAND ----------

model_rf2 = pipeline_rf2.fit(train_data)

# COMMAND ----------

model_rf2.save("/FileStore/my_folder/fitted_models/rf2_comments")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model Test Results for random forest

# COMMAND ----------

model_rf = RandomForestClassificationModel.load('/FileStore/my_folder/fitted_models/rf_comments')

# COMMAND ----------

predictions = model_rf.transform(test_data)
evaluatorRF = MulticlassClassificationEvaluator(labelCol="can_gild", predictionCol="prediction", metricName="accuracy")
accuracy = evaluatorRF.evaluate(predictions)
print("Accuracy = %g" % accuracy)
print("Test Error = %g" % (1.0 - accuracy))

# COMMAND ----------

evaluatorRF = BinaryClassificationEvaluator(labelCol="can_gild", rawPredictionCol="prediction", metricName="areaUnderROC")
roc_result = evaluatorRF.evaluate(predictions)
print("ROC Value: ", roc_result)

# COMMAND ----------

model_rf2 = RandomForestClassificationModel.load('/FileStore/my_folder/fitted_models/rf2_comments')

# COMMAND ----------

predictions2 = model_rf2.transform(test_data)
evaluatorRF2 = MulticlassClassificationEvaluator(labelCol="can_gild", predictionCol="prediction", metricName="accuracy")
accuracy2 = evaluatorRF2.evaluate(predictions2)
print("Accuracy = %g" % accuracy2)
print("Test Error = %g" % (1.0 - accuracy2))

# COMMAND ----------

evaluatorRF2 = BinaryClassificationEvaluator(labelCol="can_gild", rawPredictionCol="prediction", metricName="areaUnderROC")
roc_result2 = evaluatorRF2.evaluate(predictions2)
print("ROC Value: ", roc_result2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create pipeline and train a decision tree and an SVM model to classify if the comment is gilded

# COMMAND ----------

from pyspark.ml.classification import DecisionTreeClassifier

dt1 = DecisionTreeClassifier(labelCol="can_gild", featuresCol="features", max_depth=5)

pipeline_rf3 = Pipeline(stages=[stringIndexer_topic, 
                               stringIndexer_sentiment, 
                               onehot_topic,
                               onehot_sentiment,
                               onehot_day_of_week,
                               onehot_hour,
                               vectorAssembler_features, 
                               dt1])

model_rf3 = pipeline_rf3.fit(train_data)

# COMMAND ----------

model_rf3.save("/FileStore/my_folder/fitted_models/dt_comments")

# COMMAND ----------

dt2 = DecisionTreeClassifier(labelCol="can_gild", featuresCol="features", max_depth=8)

pipeline_rf3_ = Pipeline(stages=[stringIndexer_topic, 
                               stringIndexer_sentiment, 
                               onehot_topic,
                               onehot_sentiment,
                               onehot_day_of_week,
                               onehot_hour,
                               vectorAssembler_features, 
                               dt2])

model_rf3_ = pipeline_rf3_.fit(train_data)

# COMMAND ----------

from pyspark.ml.classification import LinearSVC
lsvc1 = LinearSVC(labelCol="can_gild", featuresCol="features", maxIter=5, regParam=0.1)

pipeline_rf4 = Pipeline(stages=[stringIndexer_topic, 
                               stringIndexer_sentiment,
                               onehot_topic,
                               onehot_sentiment,
                               onehot_day_of_week,
                               onehot_hour,
                               vectorAssembler_features, 
                               lsvc1])

model_rf4 = pipeline_rf4.fit(train_data)

# COMMAND ----------

model_rf4.save("/FileStore/my_folder/fitted_models/svm_comments")

# COMMAND ----------

lsvc2 = LinearSVC(labelCol="can_gild", featuresCol="features", maxIter=5, regParam=0.01)

pipeline_rf4_ = Pipeline(stages=[stringIndexer_topic, 
                               stringIndexer_sentiment,
                               onehot_topic,
                               onehot_sentiment,
                               onehot_day_of_week,
                               onehot_hour,
                               vectorAssembler_features, 
                               lsvc2])

model_rf4_ = pipeline_rf4_.fit(train_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model Test Results for Decision Tree and SVM

# COMMAND ----------

model_rf3 = DecisionTreeClassificationModel.load('/FileStore/my_folder/fitted_models/dt_comments')

# COMMAND ----------

predictions3 = model_rf3.transform(test_data)
evaluatorRF3 = MulticlassClassificationEvaluator(labelCol="can_gild", predictionCol="prediction", metricName="accuracy")
accuracy3 = evaluatorRF3.evaluate(predictions3)
print("Accuracy = %g" % accuracy3)
print("Test Error = %g" % (1.0 - accuracy3))

# COMMAND ----------

evaluatorRF3 = BinaryClassificationEvaluator(labelCol="can_gild", rawPredictionCol="prediction", metricName="areaUnderROC")
roc_result3 = evaluatorRF3.evaluate(predictions3)
print("ROC Value: ", roc_result3)

# COMMAND ----------

predictions3_ = model_rf3_.transform(test_data)
evaluatorRF3 = MulticlassClassificationEvaluator(labelCol="can_gild", predictionCol="prediction", metricName="accuracy")
accuracy3_ = evaluatorRF3.evaluate(predictions3_)
print("Accuracy = %g" % accuracy3_)
print("Test Error = %g" % (1.0 - accuracy3_))

# COMMAND ----------

evaluatorRF3 = BinaryClassificationEvaluator(labelCol="can_gild", rawPredictionCol="prediction", metricName="areaUnderROC")
roc_result3_ = evaluatorRF3.evaluate(predictions3_)
print("ROC Value: ", roc_result3_)

# COMMAND ----------

model_rf4 = LinearSVCModel.load('/FileStore/my_folder/fitted_models/svm_comments')

# COMMAND ----------

predictions4 = model_rf4.transform(test_data)
evaluatorRF4 = MulticlassClassificationEvaluator(labelCol="can_gild", predictionCol="prediction", metricName="accuracy")
accuracy4 = evaluatorRF4.evaluate(predictions4)
print("Accuracy = %g" % accuracy4)
print("Test Error = %g" % (1.0 - accuracy4))

# COMMAND ----------

evaluatorRF4 = BinaryClassificationEvaluator(labelCol="can_gild", rawPredictionCol="prediction", metricName="areaUnderROC")
roc_result4 = evaluatorRF4.evaluate(predictions4)
print("ROC Value: ", roc_result4)

# COMMAND ----------

predictions4_ = model_rf4_.transform(test_data)
evaluatorRF4 = MulticlassClassificationEvaluator(labelCol="can_gild", predictionCol="prediction", metricName="accuracy")
accuracy4_ = evaluatorRF4.evaluate(predictions4_)
print("Accuracy = %g" % accuracy4_)
print("Test Error = %g" % (1.0 - accuracy4_))

# COMMAND ----------

evaluatorRF4 = BinaryClassificationEvaluator(labelCol="can_gild", rawPredictionCol="prediction", metricName="areaUnderROC")
roc_result4_ = evaluatorRF4.evaluate(predictions4_)
print("ROC Value: ", roc_result4_)

# COMMAND ----------

# MAGIC %md
# MAGIC # Chart and Table

# COMMAND ----------

# MAGIC %md
# MAGIC Among all the four models in this problem, the decision tree model performs the best. The following chart is the confusion matrix of the decision tree model.

# COMMAND ----------

from sklearn.metrics import confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns

y_pred=predictions3.select("prediction").collect()
y_orig=predictions3.select("can_gild").collect()
                                                                                
cm = confusion_matrix(y_orig, y_pred)
print("Confusion Matrix:")
print(cm)

f,ax=plt.subplots()
sns.heatmap(cm,annot=True,ax=ax)

ax.set_title('confusion matrix') 
ax.set_xlabel('Prediction')
ax.set_ylabel('Label')

# COMMAND ----------

model_perf = predictions3.withColumn('DT_result', F.when(col("can_gild") == col("prediction"), 1).otherwise(0))
model_perf.groupBy('DT_result').agg(F.count('score').alias('count'),
         F.mean('score').alias('mean'),
         F.stddev('score').alias('std'),
         F.min('score').alias('min'),
         F.expr('percentile(score, array(0.25))')[0].alias('%25'),
         F.expr('percentile(score, array(0.5))')[0].alias('%50'),
         F.expr('percentile(score, array(0.75))')[0].alias('%75'),
         F.max('score').alias('max'),
         F.mean('controversiality').alias('controversial_rate')).show()

# COMMAND ----------

import pandas as pd
import altair as alt

df_result1 = model_perf.select(["DT_result", "score"]).filter(col("DT_result")==1).filter(col('score')<=30).filter(col('score')>=-8).sample(fraction = 0.001896)
df_result2 = model_perf.select(["DT_result", "score"]).filter(col("DT_result")==0).filter(col('score')<=30).filter(col('score')>=-8).sample(fraction = 0.0107)
df_visual = pd.concat([df_result1.toPandas(), df_result2.toPandas()])

alt.Chart(df_visual).mark_bar(
    opacity=0.2,
    interpolate='step'
).encode(
    alt.X('score:Q', scale=alt.Scale(domain=(-3, 12))),
    alt.Y('count()', stack=None),
    alt.Color('DT_result:N')
).properties(
    title='The Overlapping Histogram for the Result of the Decision Tree'
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Analyze Submission Data

# COMMAND ----------

train_data2, test_data2, predict_data2 = submission_ML.randomSplit([0.8, 0.18, 0.02], 24)

# COMMAND ----------

# Convert all the string fields to numeric indices by StringIndexer estimator.
stringIndexer_topic = StringIndexer(inputCol="submission_topic", outputCol="topic_ix")
stringIndexer_sentiment = StringIndexer(inputCol="sentiment", outputCol="sentiment_ix")

# COMMAND ----------

# Convert the index variables that have more than two levels by the function OneHotEncoder.
onehot_topic = OneHotEncoder(inputCol="topic_ix", outputCol="topic_vec")
onehot_sentiment = OneHotEncoder(inputCol="sentiment_ix", outputCol="sentiment_vec")
onehot_day_of_week = OneHotEncoder(inputCol="Day_of_week", outputCol="day_of_week_vec")
onehot_hour = OneHotEncoder(inputCol="Hour", outputCol="hour_vec")

# COMMAND ----------

vectorAssembler_features2 = VectorAssembler(
    inputCols=["day_of_week_vec","hour_vec","topic_vec","sentiment_vec","score","num_comments","stickied","length"], 
    outputCol= "features")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Random Forest

# COMMAND ----------

rf3 = RandomForestClassifier(labelCol="over_18", featuresCol="features", numTrees=200)

pipeline_rf5 = Pipeline(stages=[stringIndexer_topic, 
                               stringIndexer_sentiment, 
                               onehot_topic,
                               onehot_sentiment,
                               onehot_day_of_week,
                               onehot_hour,                              
                               vectorAssembler_features2,
                               rf3])

model_rf5 = pipeline_rf5.fit(train_data2)

predictions5 = model_rf5.transform(test_data2)
evaluatorRF5 = MulticlassClassificationEvaluator(labelCol="over_18", predictionCol="prediction", metricName="accuracy")
accuracy5 = evaluatorRF5.evaluate(predictions5)
print("Accuracy = %g" % accuracy5)
print("Test Error = %g" % (1.0 - accuracy5))

# COMMAND ----------

evaluatorRF5 = BinaryClassificationEvaluator(labelCol="over_18", rawPredictionCol="prediction", metricName="areaUnderROC")
roc_result5 = evaluatorRF5.evaluate(predictions5)
print("ROC Value: ", roc_result5)

# COMMAND ----------

rf4 = RandomForestClassifier(labelCol="over_18", featuresCol="features", numTrees=300)

pipeline_rf6 = Pipeline(stages=[stringIndexer_topic, 
                               stringIndexer_sentiment, 
                               onehot_topic,
                               onehot_sentiment,
                               onehot_day_of_week,
                               onehot_hour,                              
                               vectorAssembler_features2,
                               rf4])

model_rf6 = pipeline_rf6.fit(train_data2)

predictions6 = model_rf6.transform(test_data2)
evaluatorRF6 = MulticlassClassificationEvaluator(labelCol="over_18", predictionCol="prediction", metricName="accuracy")
accuracy6 = evaluatorRF6.evaluate(predictions6)
print("Accuracy = %g" % accuracy6)
print("Test Error = %g" % (1.0 - accuracy6))

# COMMAND ----------

evaluatorRF6 = BinaryClassificationEvaluator(labelCol="over_18", rawPredictionCol="prediction", metricName="areaUnderROC")
roc_result6 = evaluatorRF6.evaluate(predictions6)
print("ROC Value: ", roc_result6)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decision Tree

# COMMAND ----------

from pyspark.ml.classification import DecisionTreeClassifier

dt2 = DecisionTreeClassifier(labelCol="over_18", featuresCol="features", max_depth=5)

pipeline_rf7 = Pipeline(stages=[stringIndexer_topic, 
                               stringIndexer_sentiment, 
                               onehot_topic,
                               onehot_sentiment,
                               onehot_day_of_week,
                               onehot_hour,                              
                               vectorAssembler_features2,
                               dt2])

model_rf7 = pipeline_rf7.fit(train_data2)

predictions7 = model_rf7.transform(test_data2)
evaluatorRF7 = MulticlassClassificationEvaluator(labelCol="over_18", predictionCol="prediction", metricName="accuracy")
accuracy7 = evaluatorRF7.evaluate(predictions7)
print("Accuracy = %g" % accuracy7)
print("Test Error = %g" % (1.0 - accuracy7))

# COMMAND ----------

evaluatorRF7 = BinaryClassificationEvaluator(labelCol="over_18", rawPredictionCol="prediction", metricName="areaUnderROC")
roc_result7 = evaluatorRF7.evaluate(predictions7)
print("ROC Value: ", roc_result7)

# COMMAND ----------

model_rf7.save("/FileStore/my_folder/fitted_models/dt_submission")

# COMMAND ----------

from pyspark.ml.classification import DecisionTreeClassifier

dt2_1 = DecisionTreeClassifier(labelCol="over_18", featuresCol="features", max_depth=8)

pipeline_rf7_1 = Pipeline(stages=[stringIndexer_topic, 
                               stringIndexer_sentiment, 
                               onehot_topic,
                               onehot_sentiment,
                               onehot_day_of_week,
                               onehot_hour,                              
                               vectorAssembler_features2,
                               dt2_1])

model_rf7_1 = pipeline_rf7_1.fit(train_data2)

predictions7_1 = model_rf7_1.transform(test_data2)
evaluatorRF7_1 = MulticlassClassificationEvaluator(labelCol="over_18", predictionCol="prediction", metricName="accuracy")
accuracy7_1 = evaluatorRF7.evaluate(predictions7_1)
print("Accuracy = %g" % accuracy7_1)
print("Test Error = %g" % (1.0 - accuracy7_1))

# COMMAND ----------

evaluatorRF7 = BinaryClassificationEvaluator(labelCol="over_18", rawPredictionCol="prediction", metricName="areaUnderROC")
roc_result7_1 = evaluatorRF7.evaluate(predictions7_1)
print("ROC Value: ", roc_result7_1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SVM Model

# COMMAND ----------

from pyspark.ml.classification import LinearSVC

lsvc4 = LinearSVC(labelCol="over_18", featuresCol="features", maxIter=5, regParam=0.1)

pipeline_rf8 = Pipeline(stages=[stringIndexer_topic, 
                               stringIndexer_sentiment, 
                               onehot_topic,
                               onehot_sentiment,
                               onehot_day_of_week,
                               onehot_hour,                              
                               vectorAssembler_features2,
                               lsvc4])

model_rf8 = pipeline_rf8.fit(train_data2)

predictions8 = model_rf8.transform(test_data2)
evaluatorRF8 = MulticlassClassificationEvaluator(labelCol="over_18", predictionCol="prediction", metricName="accuracy")
accuracy8 = evaluatorRF8.evaluate(predictions8)
print("Accuracy = %g" % accuracy8)
print("Test Error = %g" % (1.0 - accuracy8))

# COMMAND ----------

evaluatorRF8 = BinaryClassificationEvaluator(labelCol="over_18", rawPredictionCol="prediction", metricName="areaUnderROC")
roc_result8 = evaluatorRF8.evaluate(predictions8)
print("ROC Value: ", roc_result8)

# COMMAND ----------

from pyspark.ml.classification import LinearSVC

lsvc4_1 = LinearSVC(labelCol="over_18", featuresCol="features", maxIter=5, regParam=0.01)

pipeline_rf8_1 = Pipeline(stages=[stringIndexer_topic, 
                               stringIndexer_sentiment, 
                               onehot_topic,
                               onehot_sentiment,
                               onehot_day_of_week,
                               onehot_hour,                              
                               vectorAssembler_features2,
                               lsvc4_1])

model_rf8_1 = pipeline_rf8_1.fit(train_data2)

predictions8_1 = model_rf8.transform(test_data2)
evaluatorRF8_1 = MulticlassClassificationEvaluator(labelCol="over_18", predictionCol="prediction", metricName="accuracy")
accuracy8_1 = evaluatorRF8_1.evaluate(predictions8_1)
print("Accuracy = %g" % accuracy8_1)
print("Test Error = %g" % (1.0 - accuracy8_1))

# COMMAND ----------

evaluatorRF8 = BinaryClassificationEvaluator(labelCol="over_18", rawPredictionCol="prediction", metricName="areaUnderROC")
roc_result8_1 = evaluatorRF8.evaluate(predictions8_1)
print("ROC Value: ", roc_result8_1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Images for Feature Importance

# COMMAND ----------

dtModel= pipeline_rf7.fit(train_data2)
va = dtModel.stages[-2]
tree = dtModel.stages[-1]
importance= list(zip(va.getInputCols(), tree.featureImportances))
importance_df= pd.DataFrame(importance)
importance_df.columns=['factor','importance_score']

# Create a set of colors
import matplotlib.pyplot as plt
colors = ['#4F6272', '#B7C3F3', '#DD7596', '#8EB897']
importance_df= importance_df.loc[importance_df['importance_score']!=0,:]
# Use it thanks to the color argument
plt.pie(importance_df['importance_score'], autopct='%.2f%%',labels=importance_df['factor'], labeldistance=1.15, wedgeprops = { 'linewidth' : 1, 'edgecolor' : 'white' }, colors=colors)

# COMMAND ----------

dtModel= pipeline_rf7.fit(train_data)
va = dtModel.stages[-2]
tree = dtModel.stages[-1]
importance= list(zip(va.getInputCols(), tree.featureImportances))
importance_df= pd.DataFrame(importance)
importance_df.columns=['factor','importance_score']

# Create a set of colors
import matplotlib.pyplot as plt
colors = ['#4F6272', '#B7C3F3', '#DD7596', '#8EB897']
importance_df= importance_df.loc[importance_df['importance_score']!=0,:]
# Use it thanks to the color argument
plt.pie(importance_df['importance_score'], autopct='%.2f%%',labels=importance_df['factor'], labeldistance=1.15, wedgeprops = { 'linewidth' : 1, 'edgecolor' : 'white' }, colors=colors)
