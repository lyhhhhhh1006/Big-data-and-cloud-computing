# Databricks notebook source
# MAGIC %md
# MAGIC ### Setup for SparkNLP & Read the Entire Dataset

# COMMAND ----------

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

##Check Missing Values
for col in datingapp_df_submission.columns:
    print(col, "with null values: ", datingapp_df_submission.filter(datingapp_df_submission[col].isNull()).count())

# COMMAND ----------

comments_final.limit(5).toPandas().head()

# COMMAND ----------

submissions_final.limit(5).toPandas().head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean text data

# COMMAND ----------

import nltk
nltk.download('stopwords')

# COMMAND ----------

from nltk.corpus import stopwords
eng_stopwords = stopwords.words('english')

documentAssembler = DocumentAssembler() \
     .setInputCol('body') \
     .setOutputCol('document')
tokenizer = Tokenizer() \
     .setInputCols(['document']) \
     .setOutputCol('token')
# note normalizer defaults to changing all words to lowercase.
# Use .setLowercase(False) to maintain input case.
normalizer = Normalizer() \
     .setInputCols(['token']) \
     .setOutputCol('normalized') \
     .setLowercase(True)
# note that lemmatizer needs a dictionary. So I used the pre-trained
# model (note that it defaults to english)
lemmatizer = LemmatizerModel.pretrained() \
     .setInputCols(['normalized']) \
     .setOutputCol('lemma')
stopwords_cleaner = StopWordsCleaner() \
     .setInputCols(['lemma']) \
     .setOutputCol('clean_lemma') \
     .setCaseSensitive(False) \
     .setStopWords(eng_stopwords)
#stemmer = Stemmer() \
#    .setInputCols(["cleanTokens"]) \
#    .setOutputCol("stem")# Convert custom document structure to array of tokens.
# finisher converts tokens to human-readable output
finisher = Finisher() \
     .setInputCols(['clean_lemma']) \
     .setCleanAnnotations(False)

# COMMAND ----------

pipeline = Pipeline() \
     .setStages([
           documentAssembler,
           tokenizer,
           normalizer,
           lemmatizer,
           stopwords_cleaner,
           finisher
     ])

# COMMAND ----------

df_text = advice_df_comments.select("body")
#result = pipeline.fit(df_text).transform(df_text)
#result.cache

# COMMAND ----------

result.printSchema()

# COMMAND ----------

result.show()

# COMMAND ----------

result.select('finished_clean_lemma').show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add external data
# MAGIC ##### Acquire, clean, and merge your external data source(s) onto your Reddit data. Produce appropriate charts/tables showing the distribution of your external data alongside your Reddit data.

# COMMAND ----------

cidea_df = spark.read.csv("/FileStore/crazy_ideas.csv",header = True)
cidea_df.printSchema()

# COMMAND ----------

cidea_df.show(10)

# COMMAND ----------

print("cidea_df has ",cidea_df.count(), "Rows",  len(cidea_df.columns), "Columns")

# COMMAND ----------

cidea_1 = cidea_df.select('name','selftext')
comments_final_merged = comments_final.join(cidea_1, comments_final["link_id"] == cidea_1["name"],"LEFT")

# COMMAND ----------

comments_final_merged.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conduct your natural language processing work
# MAGIC ##### the most common words overall or over time

# COMMAND ----------

words = result.select('finished_clean_lemma').withColumn('exploded_text', f.explode(col('finished_clean_lemma')))
words.cache
words.select("exploded_text").show(5, truncate = False)

# COMMAND ----------

counts = words.select('exploded_text').groupby('exploded_text').count()
counts.cache
counts.show()

# COMMAND ----------

counts = counts.sort('count', ascending=False)
counts.show()

# COMMAND ----------

counts.show(50)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conduct your natural language processing work
# MAGIC ##### the distribution of text lengths

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.functions import col, lit, count, isnan, when

# COMMAND ----------

advice_df_comments = advice_df_comments.withColumn("length", f.length(col("body")))

# COMMAND ----------

len=advice_df_comments.limit(10000)

# COMMAND ----------

import matplotlib.pyplot as plt
plt.hist(len.select('length').toPandas(), range(0,300))
plt.title("Distribution of length of texts")
plt.xlabel("Length")
plt.ylabel("Frequency")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conduct your natural language processing work
# MAGIC ##### important words according to TF-IDF

# COMMAND ----------

from pyspark.ml.feature import HashingTF, IDF

# To generate Term Frequency
hashingTF = HashingTF(inputCol="finished_clean_lemma", outputCol="rawFeatures", numFeatures=1000)

# To generate Inverse Document Frequency
idf = IDF(inputCol="rawFeatures", outputCol="features", minDocFreq=5)

tfidfpipeline = Pipeline().setStages([hashingTF, idf])

result_tfidf = tfidfpipeline.fit(result).transform(result)

# COMMAND ----------

result_tfidf.cache
result_tfidf.printSchema()

# COMMAND ----------

result_tfidf.select('finished_clean_lemma', 'features').show(truncate = False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Identify important keywords for your reddit data

# COMMAND ----------

from pyspark.sql.types import DoubleType, ArrayType

# Extract the "values" field from "features", which is the owrd weight
sparse_values = f.udf(lambda v: v.values.tolist(), ArrayType(DoubleType()))

words_weight = result_tfidf.select('finished_clean_lemma', 'features') \
.withColumn("features", sparse_values("features")) \
.withColumn("new", f.arrays_zip("finished_clean_lemma", "features")) \
.withColumn("new", f.explode("new")) \
.select(f.col("new.finished_clean_lemma").alias("word"), f.col("new.features").alias("weight"))

words_weight.cache

# COMMAND ----------

words_weight_sum = words_weight.na.drop().groupby('word').sum("weight").sort("sum(weight)", ascending=False)
words_weight_sum.cache
words_weight_sum.show(100)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Use regex searches to create at least two dummy variables to identify comments of particular topics.

# COMMAND ----------

advice_df_comments_dum = advice_df_comments

# COMMAND ----------

advice_df_comments_dum = advice_df_comments_dum.withColumn('love', advice_df_comments_dum.body.like('%touch%') | advice_df_comments_dum.body.like('%kiss%') | advice_df_comments_dum.body.like('%hug%') | advice_df_comments_dum.body.like('%cuddle%') | advice_df_comments_dum.body.like('%sex%'))
advice_df_comments_dum = advice_df_comments_dum.withColumn('dating', advice_df_comments_dum.body.like('%restaurant%') | advice_df_comments_dum.body.like('%coffee%') | advice_df_comments_dum.body.like('%dinner%') | advice_df_comments_dum.body.like('%brunch%') | advice_df_comments_dum.body.like('%vibe%') | advice_df_comments_dum.body.like('%chat%') | advice_df_comments_dum.body.like('%match%'))
advice_df_comments_dum = advice_df_comments_dum.withColumn('entertainment', advice_df_comments_dum.body.like('%game%') | advice_df_comments_dum.body.like('%movie%') | advice_df_comments_dum.body.like('%video%') | advice_df_comments_dum.body.like('%book%'))
advice_df_comments_dum = advice_df_comments_dum.withColumn('dailylife', advice_df_comments_dum.body.like('%food%') | advice_df_comments_dum.body.like('%drink%') | advice_df_comments_dum.body.like('%sleep%') | advice_df_comments_dum.body.like('%company%'))
advice_df_comments_dum = advice_df_comments_dum.withColumn('religion', advice_df_comments_dum.body.like('%religion%'))
advice_df_comments_dum = advice_df_comments_dum.withColumn('relationship', advice_df_comments_dum.body.like('%parent%') | advice_df_comments_dum.body.like('%family%') | advice_df_comments_dum.body.like('%couple%'))

# COMMAND ----------

advice_df_comments_dum = advice_df_comments_dum.withColumn('topic', F.when(col("love") == True, "love") \
                                     .when(col("dating") == True, "dating") \
                                     .when(col("entertainment") == True, "entertainment") \
                                     .when(col("dailylife") == True, "dailylife") \
                                     .when(col("religion") == True, "religion") \
                                     .when(col("relationship") == True, "relationship") \
                                     .otherwise("none"))

advice_df_comments_dum = advice_df_comments_dum.drop("love", "dating", "entertainment", "dailylife", "religion", "relationship")

# COMMAND ----------

advice_df_comments_dum.groupby("topic").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Summary table for 'topic'

# COMMAND ----------

topic_summary = advice_df_comments_dum.select('topic','comment_score').na.drop() \
                        .filter(col("topic")!="none") \
                        .groupby("topic")\
                        .agg(f.count('comment_score').alias('count'),
                             f.mean('comment_score').alias('mean'),
                             f.stddev('comment_score').alias('std'),
                             f.min('comment_score').alias('min'),
                             f.expr('percentile(comment_score, array(0.25))')[0].alias('%25'),
                             f.expr('percentile(comment_score, array(0.5))')[0].alias('%50'),
                             f.expr('percentile(comment_score, array(0.75))')[0].alias('%75'),
                             f.max('comment_score').alias('max'))

topic_summary.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build a sentiment model

# COMMAND ----------

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

#Advice Sentiment
advice_result_nlp = nlpPipeline.fit(advice_df_comments).transform(advice_df_comments)
advice_result_nlp.cache

# COMMAND ----------

advice_comments_nlp = advice_result_nlp.withColumn('sentiment', col('sentiment.result')[0]).drop('sentence_embeddings').drop('document')
advice_comments_nlp.printSchema()

# COMMAND ----------

advice_sentiment_summary = advice_comments_nlp.select('sentiment', 'comment_score').groupby("sentiment").agg(count('comment_score'), f.mean('comment_score'))
advice_sentiment_summary.dropna().show()

# COMMAND ----------

#senti_summary = pd.DataFrame(data = {"sentiment": ["Negative", "Positive", "Neural"], "Count": [10224959, 7858955, 177809], "Avg_score": [10.36717105662722, 9.469905986228449, 12.202222609654179]})

import matplotlib.pyplot as plt

# Data to plot
labels = "Negative", "Positive", "Neutral"
sizes = [10224959, 7858955, 177809]

# Definitions
colors = ['lightcoral','gold', 'yellowgreen', 'lightskyblue']

# explode 1st slice
explode = (0, 0.2, 0)

# Plot
plt.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.1f%%', shadow=True, startangle=140)

plt.legend(labels, loc="lower right")
plt.title("Sentiment Distribution of Comments on Advice for Dating")

plt.axis('equal')
plt.show()

# COMMAND ----------

import altair as alt

alt.Chart(advice_comments_nlp.sample(fraction = 0.0005).toPandas()).transform_density(
    'score',
    as_=['score', 'density'],
    extent=[-5, 20],
    groupby=['sentiment']
).mark_area(orient='horizontal').encode(
    y='score:Q',
    color='sentiment:N',
    x=alt.X(
        'density:Q',
        stack='center',
        impute=None,
        title=None,
        axis=alt.Axis(labels=False, values=[0],grid=False, ticks=True),
    ),
    column=alt.Column(
        'sentiment:N',
        header=alt.Header(
            titleOrient='bottom',
            labelOrient='bottom',
            labelPadding=0,
        ),
    )
).properties(
    width=100
).configure_facet(
    spacing=0
).configure_view(
    stroke=None
)

# COMMAND ----------

import seaborn as sns
advice_comments_nlp_small = advice_comments_nlp.sample(fraction = 0.0005).toPandas()

# COMMAND ----------

advice_comments_nlp_small.head()

# COMMAND ----------

df1 = advice_comments_nlp_small[["comment_score","sentiment"]]
df1.head()

# COMMAND ----------

df2 = df1[(df1["comment_score"] >= -100) &  (df1["comment_score"] <= 200)]

# COMMAND ----------

df2 = df2.pivot(columns = "sentiment", values = "comment_score" )
df2.head()

# COMMAND ----------

vals, names, xs = [],[],[]
for i, col in enumerate(df2.columns):
    vals.append(df2[col].values)
    names.append(col)
    xs.append(np.random.normal(i + 1, 0.03, df2[col].values.shape[0]))  # adds jitter to the data points - can be adjusted
    
plt.boxplot(vals, labels=names)
palette = ['#ff2700', '#6bbe41', '#f6de09']
for x, val, c in zip(xs, vals, palette):
    plt.scatter(x, val, alpha=0.4, color=c)
plt.title("Relationship Between Sentiment and Scores")

plt.show()    


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Summary table for 'topic' and 'sentiment'

# COMMAND ----------

#Advice Sentiment with dummy variables
advice_dum_nlp = nlpPipeline.fit(advice_df_comments_dum).transform(advice_df_comments_dum)
advice_dum_nlp.cache

# COMMAND ----------

advice_dum_nlp = advice_dum_nlp.withColumn('sentiment', col('sentiment.result')[0]).drop('sentence_embeddings').drop('document')
advice_dum_nlp.printSchema()

# COMMAND ----------

advice_dum_nlp=advice_dum_nlp.dropna()

# COMMAND ----------

advice_dum_nlp_senti = advice_dum_nlp.select("sentiment", "topic")\
                                    .filter(col("topic")!="none") \
                                    .groupby("topic", "sentiment").count() \
                                    .sort("topic", "sentiment")
advice_dum_nlp_senti.dropna().show()

# COMMAND ----------

#pd_topic_senti = advice_dum_nlp_senti.dropna().toPandas()
import altair as alt

alt.Chart(pd_topic_senti,title="Sentiment Statistics by Topic").mark_bar().encode(
    x=alt.X('count:Q', stack='zero'),
    y=alt.Y('topic:N'),
    color=alt.Color('sentiment', scale=alt.Scale(scheme='pastel1')), 
    order=alt.Order(
      # Sort the segments of the bars by this field
      'topic',
      sort='ascending'
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Output

# COMMAND ----------

advice_dum_nlp.write.parquet("/Dataset/advice_nlp_with_dum")
