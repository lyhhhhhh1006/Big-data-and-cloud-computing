# Databricks notebook source
# MAGIC %md 
# MAGIC ### Make a project plan for my Reddit data with 10 topics that we plan on exploring including business goal as well as technical proposal

# COMMAND ----------

# MAGIC %md
# MAGIC - 1: What is the most popular dating APP?
# MAGIC   - Use NLP to identify the posts that mentioned one and more than one dating apps. Conduct sentiment analysis of the posts to assign positive or negative values to the dating apps mentioned in posts. Present the findings for the sentiment analysis for the top 3 popular dating apps.
# MAGIC - 2: What kind of dating/relationship is the most popular nowadays?
# MAGIC   - Utilize NLP to do the topic popularity calculation: Weighting the words with the hotness of the main posts, and the heat of the main post will take into account the following factors like the number of subreddits, score, time of the publication (The longer the release time, the higher the heat decay). Using Wordcloud to show the hottest words among the selected subreddits and extract key words to determine which types of dates are most popular. Age and gender factors can also be considered to categorize the study.
# MAGIC - 3: What is the sentiment composition of the posts in the subreddit and in each topic?
# MAGIC   - Visualize the pattern of text length of the comment data. Conduct the distribution of the length of text. Present text length of the most comments and the characteristic of reddit.
# MAGIC - 4: How are the scores of the posts/comments related to topics and sentiments?
# MAGIC   - Use NLP to identify the posts and comments and vectorize the dataset. Create a pipeline for sentiment analysis based on SparkNLP. Plot to demonstrate the difference of the distributions by sentiment and the score. 
# MAGIC - 5: How does people's attention about relationship health change over time?
# MAGIC   - Leverage Time series analysis to help understanding the underlying attention patterns of relationship over time. Using data visualizations, we want to see seasonal trends and year deviation and dig deeper into why these trends occur. When analyzing data over consistent intervals, we try to use time series forecasting to predict the likelihood of future dating events.
# MAGIC - 6: What is the difference between men's interest about women and women's interest about men?
# MAGIC   - Use NLP to collect and tokenize the submission with the most comment posts in "Ask Man" and "Ask Women" subreddit seperately. Conduct sentiment analysis of the submission to assign positive or negative values to questions redditors asked. Present top 10 positive and negative submission. Make the same process on comment part and compare the difference between men’s aspect and women’s aspect.
# MAGIC - 7: Which variables will have an impact on the submission score? what is the relationship between score and controversies?
# MAGIC   - Use NLP to transform string variables to numbers and assemble feature vectors. Build up a pipeline to transform all data. Use Linear Regression to predict score with feature selecting and tuning the model.
# MAGIC - 8: How does the popularity of posts distribute?
# MAGIC   - Visualize the distribution of the scores of posts. Conduct the highest frequency of the posts score. Present the findings of maximum and minimum value. 
# MAGIC - 9: which age group use online dating app the most?
# MAGIC   - Generate the distribution plot for each age group using created_utc which stands for the utc timestamp of the post time of the main post.
# MAGIC   Conduct statistical analysis for respondents from different age groups. Present sorted count for reddit users who have been using dating apps.
# MAGIC - 10: While talking about dating advices, which are the most frequent words?
# MAGIC   - Utilize NLP for word association calculation to examine the association between words by calculating the probability of simultaneous occurrence of words in subreddit. For example, the most associated word in a dinner date may be the most important suggestion in a dinner date. Can also use Association Rule Mining to calculate the relationship between words.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the data
# MAGIC There are two folders `comments` and `submissions`, we will read both of them into separate Spark dataframes. The raw data is in [parquet format](https://www.databricks.com/glossary/what-is-parquet#:~:text=What%20is%20Parquet%3F,handle%20complex%20data%20in%20bulk.).

# COMMAND ----------

dbutils.fs.ls("abfss://anly502@marckvaismanblob.dfs.core.windows.net/reddit/parquet")

# COMMAND ----------

comments = spark.read.parquet("abfss://anly502@marckvaismanblob.dfs.core.windows.net/reddit/parquet/comments")
submissions = spark.read.parquet("abfss://anly502@marckvaismanblob.dfs.core.windows.net/reddit/parquet/submissions")

# COMMAND ----------

#comments_row_count = comments.count()
#comment_col_count = len(comments.columns)

# COMMAND ----------

comments.show()

# COMMAND ----------

comments.printSchema()

# COMMAND ----------

submissions.show()

# COMMAND ----------

submissions.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Conduct your exploratory data analysis.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Update submissions and comments dataset by dropping useless or overlapped columns and changing column names

# COMMAND ----------

submissions_new = submissions.select('id','permalink','author','created_utc','subreddit','subreddit_id','selftext','title','num_comments','score','is_self',                              'over_18','edited','domain','stickied','locked','retrieved_on')
comments_new = comments.drop(comments.distinguished)\
                        .drop(comments.is_submitter)\
                        .drop(comments.permalink)\
                        .drop(comments.author_flair_css_class)\
                        .drop(comments.author_flair_text)\
                        .withColumnRenamed('id','comment_id')\
                        .withColumnRenamed('subreddit','comment_subreddit')\
                        .withColumnRenamed('score','comment_score')\
                        .withColumnRenamed('created_utc','comment_created_utc')

# COMMAND ----------

comments_new.printSchema()

# COMMAND ----------

submissions_new.printSchema()

# COMMAND ----------

for col in submissions_new.columns:
    print(col, "with null values: ", submissions_new.filter(submissions_new[col].isNull()).count())

# COMMAND ----------

for col in comments_new.columns:
    print(col, "with null values: ", comments_new.filter(comments_new[col].isNull()).count())

# COMMAND ----------

print("Comments_new Dataframe has ", comments_new.count(), "Rows",  len(comments_new.columns), "Columns")
print("Submission_new Dataframe has ", submissions_new.count(), "Rows",  len(submissions_new.columns), "Columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Drop columns which contain too many null values or are useless

# COMMAND ----------

#Delete "retrieve_on", because it contains too many null value and the crawling timestamp is usless for our analysis.
submissions_final = submissions_new.drop(submissions_new.retrieved_on) 

# COMMAND ----------

#Delete "retrieve_on" and "author_cakeday", because these variables contain too many null value and the crawling timestamp is usless for our analysis.
comments_final = comments_new.drop(comments_new.retrieved_on).drop(comments_new.author_cakeday)

# COMMAND ----------

# MAGIC %md
# MAGIC ######  Delete rows with null values for all datasets

# COMMAND ----------

#Delete rows where "edited" and "domain" contain null value because these two columns contains very few null values and are significant to our analysis.
submissions_final = submissions_final.dropna()

# COMMAND ----------

submissions_final.show(5)

# COMMAND ----------

comments_final.show(5)

# COMMAND ----------

for col in submissions_final.columns:
    print(col, "with null values: ", submissions_final.filter(submissions_final[col].isNull()).count())

# COMMAND ----------

for col in comments_final.columns:
    print(col, "with null values: ", comments_final.filter(comments_final[col].isNull()).count())

# COMMAND ----------

print("Comments_final Dataframe has ", comments_final.count(), "Rows",  len(comments_final.columns), "Columns")
print("Submissions_final Dataframe has ", submissions_final.count(), "Rows",  len(submissions_final.columns), "Columns")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Report on the basic info about your dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC After doing some basic data cleaning such as dropping useless or overlapped columns and renamed columns, our dataset contains updated submissions and comments datasets. `comments_final` contains 14 columns and 4473556762 rows in total.`subumissions_final` contains 16 columns and 633922939 rows. `num_comments` represents the number of comments associated with this submission, `score` represents the score of submission and is the number of upvotes minus the number of downvotes, `comment_score` represents the score of comments, and `controversiality` represents number that indicates whether the comment is controversial. Those four variables show how popular the submissions and comments are. In addition, `created_utc` refers to the timestamp of the submission’s creation, `over_18` indicates whether the submission is Not-Safe-For-Work.

# COMMAND ----------

#comments_final.write.parquet("/Dataset/comments_final")
#submissions_final.write.parquet("/Dataset/submissions_final")

# COMMAND ----------

comments_final = spark.read.parquet("/Dataset/comments_final")
submissions_final = spark.read.parquet("/Dataset/submissions_final")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

## Extract subreddits from submissions
datingapp_df_submission = submissions_final.filter((col("subreddit")=='Bumble') | (col("subreddit")=='Tinder')|(col("subreddit")=='hingeapp')|(col("subreddit")=='dating')| (col("subreddit")=='DatingApps')).cache()

# COMMAND ----------

relationship_df_submission = submissions_final.filter((col("subreddit")=='relationships') | (col("subreddit")=='AskMen') | (col("subreddit")=='AskWomen')).cache()

# COMMAND ----------

advice_df_submission = submissions_final.filter((col("subreddit")=='relationship_advice') | (col("subreddit")=='datingoverforty')|(col("subreddit")=='datingoverthirty')| (col("subreddit")=='datingadvice')| (col("subreddit")=='datingoverforty')| (col("subreddit")=='datingoverfifty')).cache()

# COMMAND ----------

## Extract subreddits from comments
datingapp_df_comments = comments_final.filter((col("comment_subreddit")=='Bumble') | (col("comment_subreddit")=='Tinder')|(col("comment_subreddit")=='hingeapp')|(col("comment_subreddit")=='dating')| (col("comment_subreddit")=='DatingApps')).cache()

# COMMAND ----------

relationship_df_comments = comments_final.filter((col("comment_subreddit")=='relationships') | (col("comment_subreddit")=='AskMen') | (col("comment_subreddit")=='AskWomen')).cache()

# COMMAND ----------

advice_df_comments = comments_final.filter((col("comment_subreddit")=='relationship_advice') | (col("comment_subreddit")=='datingoverforty')|(col("comment_subreddit")=='datingoverthirty')| (col("comment_subreddit")=='datingadvice')| (col("comment_subreddit")=='datingoverforty')| (col("comment_subreddit")=='datingoverfifty')).cache()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Conduct basic data quality checks! Make sure there are no missing values, check the length of the comments, remove rows of data that might be corrupted. Even if you think all your data is perfect, you still need to demonstrate that with your analysis.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Datasets overview

# COMMAND ----------

print("Dating App Submissions Dataframe has ", datingapp_df_submission.count(), "Rows",  len(datingapp_df_submission.columns), "Columns")
print("Relationship Submissions Dataframe has ", relationship_df_submission.count(), "Rows",  len(relationship_df_submission.columns), "Columns")
print("Advice Submissions Dataframe has ", advice_df_submission.count(), "Rows",  len(advice_df_submission.columns), "Columns")

# COMMAND ----------

print("Dating App Comments Dataframe has ", datingapp_df_comments.count(), "Rows",  len(datingapp_df_comments.columns), "Columns")
print("Relationship Comments Dataframe has ", relationship_df_comments.count(), "Rows",  len(relationship_df_comments.columns), "Columns")
print("Advice Comments Dataframe has ", advice_df_comments.count(), "Rows",  len(advice_df_comments.columns), "Columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Missing values check for all of the datasets we would like to use

# COMMAND ----------

for col in datingapp_df_submission.columns:
    print(col, "with null values: ", datingapp_df_submission.filter(datingapp_df_submission[col].isNull()).count())

# COMMAND ----------

for col in relationship_df_submission.columns:
    print(col, "with null values: ", relationship_df_submission.filter(relationship_df_submission[col].isNull()).count())

# COMMAND ----------

for col in advice_df_submission.columns:
    print(col, "with null values: ", advice_df_submission.filter(advice_df_submission[col].isNull()).count())

# COMMAND ----------

for col in datingapp_df_comments.columns:
    print(col, "with null values: ", advice_df_comments.filter(advice_df_comments[col].isNull()).count())

# COMMAND ----------

for col in relationship_df_comments.columns:
    print(col, "with null values: ", advice_df_comments.filter(advice_df_comments[col].isNull()).count())

# COMMAND ----------

for col in advice_df_comments.columns:
    print(col, "with null values: ", advice_df_comments.filter(advice_df_comments[col].isNull()).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Make smaller datasets

# COMMAND ----------

datingapp_df_submission_small = datingapp_df_submission.limit(10000)
relationship_df_submission_small = relationship_df_submission.limit(10000)
advice_df_submission_small = advice_df_submission.limit(10000)
datingapp_df_comments_small = datingapp_df_comments.limit(10000)
relationship_df_comments_small = relationship_df_comments.limit(10000)
advice_df_comments_small = advice_df_comments.limit(10000)

# COMMAND ----------

# Write final dataframe into parquet files
datingapp_df_submission.write.parquet("/Dataset/datingapp_df_submission")
relationship_df_submission.write.parquet("/Dataset/relationship_df_submission")
advice_df_submission.write.parquet("/Dataset/advice_df_submission")
datingapp_df_comments.write.parquet("/Dataset/datingapp_df_comments")
relationship_df_comments.write.parquet("/Dataset/relationship_df_comments")
advice_df_comments.write.parquet("/Dataset/advice_df_comments")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Produce at least 5 interesting graphs about your dataset. Think about the dimensions that are interesting for your reddit data! There are millions of choices. Make sure your graphs are connected to your business questions.

# COMMAND ----------

import seaborn as sns 
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.ticker as ticker

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Vis 1: The change of the number of people discussing 3 dating app over time

# COMMAND ----------

v1 = datingapp_df_submission.select("created_utc", "author", "subreddit", "title")
v1 = v1.withColumn("time", from_unixtime(col("created_utc"), "yyyy-MM-dd"))
v1= v1.withColumn("app", when(lower(v1.subreddit).rlike("bumble") | lower(v1.title).rlike("bumble"), "Bumble")
                              .when(lower(v1.subreddit).rlike("tinder") | lower(v1.title).rlike("tinder"), "Tinder")
                              .when(lower(v1.subreddit).rlike("hinge") | lower(v1.title).rlike("hinge"), "Hinge")
                              .otherwise("other"))
v1 = v1.withColumn('month', month(col('time')))
v1 = v1.withColumn('year', year(col('time')))
v1 = v1.withColumn('month_year', concat_ws('-',v1.year,v1.month))
v1_df = v1.groupby(['month_year','app']).count().sort(col("month_year"))

v1_df.show(5)

# COMMAND ----------

v1_df=v1_df.toPandas()

# COMMAND ----------


sns.set_style('darkgrid')
sns.set(rc={'figure.figsize':(14,8)})

ax = sns.lineplot(data=v1_df, x ='month_year', y = 'count',
                  hue='app', palette='viridis',
                  legend='full', lw=3)

ax.xaxis.set_major_locator(ticker.MultipleLocator(4))
plt.legend(bbox_to_anchor=(1, 1))
plt.ylabel('The number of Author participating in app post')
plt.xlabel('Year-Month')
plt.title("The change of the number of people discussing 3 dating app over time")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Vis 2: The score distribution of posts with different comment degrees

# COMMAND ----------

v2_1 = advice_df_comments_small.selectExpr("comment_score as score")
v2_2 = advice_df_submission_small.select("score")
v2_1 = v2_1.withColumn("label", lit("comment"))
v2_2 = v2_2.withColumn("label", lit("submission"))

# COMMAND ----------

v2=v2_1.unionByName(v2_2)
v2=v2.toPandas()

# COMMAND ----------

b = range(-20, 40, 2)

# COMMAND ----------

sns.displot(data=v2, x="score", hue="label", kde=False,bins=b, multiple="stack", aspect=2, height=8)
plt.ylabel('Frequency')
plt.xlabel('Score')
plt.title("The score distribution of posts")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Vis 3: The change of number of posts and comment scores in a week

# COMMAND ----------

df = submissions_final_small.groupby('day_of_week') \
       .agg({'id':'size', 'score':'mean'}) \
       .rename(columns={'id':'count','score':'mean'}) \
       .reset_index()
df['post'] = 'submission'

# COMMAND ----------

df_c = comments_final_small.groupby('day_of_week') \
       .agg({'comment_id':'size', 'comment_score':'mean'}) \
       .rename(columns={'comment_id':'count','comment_score':'mean'}) \
       .reset_index()
df_c['post'] = 'comment'

# COMMAND ----------

v3 = pd.concat([df, df_c], axis=0)

# COMMAND ----------

import plotly.express as px
fig = px.scatter(v3, x="day_of_week", y="mean", color="post",
                 size='count')
fig.update_traces(
    marker=dict(symbol="star-diamond", line=dict(width=2, color="DarkSlateGrey")),
    selector=dict(mode="markers"),
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Vis 4: Submission Distribution

# COMMAND ----------

v4 = submissions_final_small
fig = px.scatter(v4, x="score", y="num_comments", color='over_18')
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Vis 5: Age & Score boxplot

# COMMAND ----------

s = advice_df_submission_small.select("subreddit", "score")\
                            .withColumn('label',lit('submission'))
c = advice_df_comments_small.select("comment_subreddit", "comment_score")\
                        .withColumn('label',lit('comment'))
v5=s.union(c)
v5= v5.withColumn("age", when(lower(v5.subreddit).rlike("thirty"),"thirty")
                              .when(lower(v5.subreddit).rlike("forty"), "forty")
                              .when(lower(v5.subreddit).rlike("fifty"), "fifty")
                              .otherwise("other"))

# COMMAND ----------

v5=v5.toPandas()
v5 = v5.loc[v5['age']!='other']

# COMMAND ----------

import seaborn as sns
sns.set_theme(style="whitegrid")
sns.set(rc={'figure.figsize':(8,6)})

sns.boxenplot(x="age", y="score",
              color="b", hue="label",
              scale="linear", data=v5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Produce at least 3 interesting summary tables about your dataset. You can decide how to split up your data into categories, time slices, etc. There are infinite ways you can make summary statistics. Be unique, creative, and interesting!

# COMMAND ----------

# read data from parquet
comments_final = spark.read.parquet("/Dataset/comments_final")
submissions_final = spark.read.parquet("/Dataset/submissions_final")

# COMMAND ----------

# create small dataset
from random import sample
comments_final_small_table = comments_final.sample(True, 0.00001, 1234)
submissions_final_small_table = submissions_final.sample(True, 0.00001, 1234)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Table 1 - relationship between controversiality and score

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.functions import *
df_com_1 = comments_final_small_table.filter(col("controversiality")==1).select("controversiality", "comment_score", 'stickied')
df_com_0 = comments_final_small_table.filter(col("controversiality")==0).select("controversiality", "comment_score", 'stickied')
df_union = df_com_1.union(df_com_0)

# COMMAND ----------

df_union.groupBy('controversiality').agg(f.count('comment_score').alias('count'),
         f.mean('comment_score').alias('mean'),
         f.stddev('comment_score').alias('std'),
         f.min('comment_score').alias('min'),
         f.expr('percentile(comment_score, array(0.25))')[0].alias('%25'),
         f.expr('percentile(comment_score, array(0.5))')[0].alias('%50'),
         f.expr('percentile(comment_score, array(0.75))')[0].alias('%75'),
         f.max('comment_score').alias('max')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC For "controversiality", we can see that the count of controversial comments is obviously less than that of uncontroversial comments, and controversial comments have smaller mean of scores but less standard deviation. This means the scores are more intensive than less controversial ones. 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Table 2 - relationship between submission score and whether the submission is Not-Safe-For-Work

# COMMAND ----------

# Select data
df_sub_T = submissions_final_small_table.filter(col("over_18")=='true').select("over_18", "score",'num_comments')
df_sub_F = submissions_final_small_table.filter(col("over_18")=='false').select("over_18", "score",'num_comments')
df_union_sub = df_sub_T.union(df_sub_F)

# COMMAND ----------

# Create the table
df_union_sub.groupBy('over_18').agg(f.count('score').alias('count'),
         f.mean('score').alias('mean'),
         f.stddev('score').alias('std'),
         f.min('score').alias('min'),
         f.expr('percentile(score, array(0.25))')[0].alias('%25'),
         f.expr('percentile(score, array(0.5))')[0].alias('%50'),
         f.expr('percentile(score, array(0.75))')[0].alias('%75'),
         f.max('score').alias('max')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC The variable `over_18` is a flag that indicates whether the submission is Not-Safe-For-Work. From the above table, we can see that the count of over 18 submission is less than that of not over 18 submissions, and over 18 have smaller mean of scores but less standard deviation. This means the scores are more intensive to those over 18 submissions.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Table 3 - Summary table of submission score, number of comments

# COMMAND ----------

submissions_final_small_table1.select('score', 'num_comments').describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC The above summary table shows the statistics summary of the score that the submission has accumulated, as well as the number of comments. We can see the range of score is very large, the min value is 0 and max value is up to 18905, but the mean score is only 52.8. And the same phenomenon applies to the number of comments. This means that though most comments did not call much attention, several did receive vast amount of upvates and comments.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Use data transformations to make AT LEAST 3 new variables that are relevant for your business questions. We cannot be more specific because this really depends on your project and what you want to explore!

# COMMAND ----------

# MAGIC %md
# MAGIC ###### New variables we have so far: datingapp_df_submission_small, relationship_df_submission_small, advice_df_submission_small, datingapp_df_comments_small, relationship_df_comments_small, advice_df_comments_small

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Create smaller datasets for comments_final and submissions_final, sample function is used here because we would like to have random samples, comments_final_small dataset has 44969 rows and submissions_final_small has 6302 rows with fraction = 0.00001

# COMMAND ----------

from random import sample
comments_final_small = comments_final.sample(True, 0.00001, 1234)
submissions_final_small = submissions_final.sample(True, 0.00001, 1234)

# COMMAND ----------

comments_final_small.limit(5).toPandas().head()

# COMMAND ----------

comments_final_small.printSchema()

# COMMAND ----------

submissions_final_small.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Since we would like to explore the fluctuations of number of main posts and comments in a week or at different time slots in a day, we create dummy variables called "day_of_week" and "hour" here using the variable "created_utc" in both comments_final_small and submissions_final_small datasets

# COMMAND ----------

from datetime import datetime

comments_final_small = comments_final_small.toPandas()
comments_final_small['day_of_week'] = comments_final_small['comment_created_utc'].map(lambda x: datetime.utcfromtimestamp(int(x)).strftime("%A"))
comments_final_small['hour'] = comments_final_small['comment_created_utc'].map(lambda x: datetime.utcfromtimestamp(int(x)).strftime("%H"))

# COMMAND ----------

submissions_final_small = submissions_final_small.toPandas()
submissions_final_small['day_of_week'] = submissions_final_small['created_utc'].map(lambda x: datetime.utcfromtimestamp(int(x)).strftime("%A"))
submissions_final_small['hour'] = submissions_final_small['created_utc'].map(lambda x: datetime.utcfromtimestamp(int(x)).strftime("%H"))

# COMMAND ----------

submissions_final_small_sp = spark.createDataFrame(submissions_final_small)
submissions_final_small_sp.select('score', 'num_comments').describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Hotness variable is created here based on different number of comments the posts have to describe the popularity of the post

# COMMAND ----------

hotness = []
for count in submissions_final_small['num_comments']:
    if count > 10 : hotness.append('popular')
    else:   
        hotness.append('unpopular')
submissions_final_small['hotness'] = hotness

# COMMAND ----------

hotness_dis = submissions_final_small.groupby(['hotness'])['id'].count().to_frame()
hotness_dis

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Also, according to the score which is number of upvotes minus downvotes from comments_final_small and submissions_final_small datasets, we split user's satisfaction of main posts into 'high', 'medium' and 'low'.

# COMMAND ----------

satisfaction = []
for score in submissions_final_small['score']:
    if score > 100 : satisfaction.append('high')
    elif 10 < score <= 100: satisfaction.append('medium')
    else:   
        satisfaction.append('low')
submissions_final_small['satisfaction'] = satisfaction

# COMMAND ----------

satisfaction_dis = submissions_final_small.groupby(['satisfaction'])['id'].count().to_frame()
satisfaction_dis

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Since there are negative values in comments_final_small dataset, we split user's satisfaction into 4 groups: 'high', 'medium', 'low' and 'dissatisfied'.

# COMMAND ----------

comments_final_small_sp = spark.createDataFrame(comments_final_small)
comments_final_small_sp.select('comment_score').describe().show()

# COMMAND ----------

satisfaction_c = []
for score_c in comments_final_small['comment_score']:
    if score_c > 10 : satisfaction_c.append('high')
    elif 2 < score_c <= 10: satisfaction_c.append('medium')
    elif 0 < score_c <= 2:  satisfaction_c.append('low')
    else:   
        satisfaction_c.append('dissatisfied')
comments_final_small['satisfaction'] = satisfaction_c

# COMMAND ----------

satisfaction_dis_c = comments_final_small.groupby(['satisfaction'])['link_id'].count().to_frame()
satisfaction_dis_c

# COMMAND ----------

# MAGIC %md
# MAGIC #### Implement regex searches for specific keywords of interest to produce dummy variables and then make statistics that are related to your business questions. Note, you DO NOT have to do textual cleaning of the data at this point. The next assignment on NLP will focus on the textual cleaning and analysis aspect.

# COMMAND ----------

submissions_final_small_1 = submissions_final.limit(10000)
comments_final_small_1 = comments_final.limit(10000)

# COMMAND ----------

import pandas as pd
pd.set_option('display.max_columns', None)
submissions_final_small_1.limit(5).toPandas().head()

# COMMAND ----------

comments_final_small_1.limit(5).toPandas().head()

# COMMAND ----------

## in order to solve the question "How does people's attention about relationship health", we decided to add a dummy variable which contains whether users care about the words like "healthy relation" using regex.
relationship_df_comments_regex = relationship_df_comments.withColumn('health_dummy', col('body').rlike("healthy relation*"))

# COMMAND ----------

relationship_df_comments_regex.filter(col('health_dummy')==True).select('body').show(5, truncate = False)

# COMMAND ----------

#Statistics
relationship_df_comments_regex.filter(col('health_dummy')==True).select('health_dummy').count()

# COMMAND ----------

relationship_df_comments_regex.filter(col('health_dummy')==False).select('health_dummy').count()

# COMMAND ----------

## Repeat regex for relationship_df_submissions dataset
relationship_df_submissions_regex = relationship_df_submission.withColumn('health_dummy', col('selftext').rlike("healthy relation*"))

# COMMAND ----------

relationship_df_submissions_regex.filter(col('health_dummy')==True).select('selftext').show(5, truncate = False)

# COMMAND ----------

#Statistics
relationship_df_submissions_regex.filter(col('health_dummy')==True).select('health_dummy').count()

# COMMAND ----------

relationship_df_submissions_regex.filter(col('health_dummy')==False).select('health_dummy').count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### External Dataset

# COMMAND ----------

# MAGIC %md
# MAGIC The External Dataset is the `appreview_df`, which contains the reviews and ratings for popular dating apps. The data is from 2017-2022 and the author acquired the data using googleplayscraper from google playstore online. Here is the data link: https://www.kaggle.com/datasets/sidharthkriplani/datingappreviews
# MAGIC 
# MAGIC We want to use this external dataset to solve the problem related to  dating app trend and popularity.

# COMMAND ----------

appreview_df = spark.read.csv("/FileStore/DatingAppReviewsDataset.csv",header = True)
appreview_df.printSchema()

# COMMAND ----------

print("appreview_df has ", appreview_df.count(), "Rows",  len(appreview_df.columns), "Columns")

# COMMAND ----------

appreview_df.show(5)

# COMMAND ----------

mytext = appreview_df.select("Review").toPandas().values 

from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt
stopwords = set(STOPWORDS)
wordcloud = WordCloud(stopwords=stopwords).generate(str(mytext))
plt.imshow(wordcloud)
plt.show()

# COMMAND ----------

# Find Outliers and delete these rows
appreview_df.select('Rating').where(appreview_df.Rating>5).count()
appreview_df = appreview_df.where(appreview_df.Rating<=5)
appreview_df = appreview_df.where(appreview_df.Rating>=0)

# COMMAND ----------

appreview_df.select('Rating').describe().show()
