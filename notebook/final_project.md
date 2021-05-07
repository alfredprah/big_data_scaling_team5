**Setting up Spark on our Colab environment.**


```python
!pip install pyspark
!pip install -U -q PyDrive
!apt install openjdk-8-jdk-headless -qq
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
```

    Requirement already satisfied: pyspark in /usr/local/lib/python3.7/dist-packages (3.1.1)
    Requirement already satisfied: py4j==0.10.9 in /usr/local/lib/python3.7/dist-packages (from pyspark) (0.10.9)
    openjdk-8-jdk-headless is already the newest version (8u282-b08-0ubuntu1~18.04).
    The following package was automatically installed and is no longer required:
      libnvidia-common-460
    Use 'apt autoremove' to remove it.
    0 upgraded, 0 newly installed, 0 to remove and 34 not upgraded.



```python
from google.colab import drive
drive.mount('/content/drive')
```

    Drive already mounted at /content/drive; to attempt to forcibly remount, call drive.mount("/content/drive", force_remount=True).



```python
import os
cur_path = "/content/drive/MyDrive/renaming_folder/final_project/"
os.chdir(cur_path)
!pwd
```

    /content/drive/MyDrive/renaming_folder/final_project



```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('final_project').getOrCreate()
```


```python
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
#sc= SparkContext()
sqlContext = SQLContext(spark)
data = spark.read.csv(cur_path + 'DisneylandReviews.csv',inferSchema=True,header=True)
```


```python
data.show()
```

    +---------+------+----------+--------------------+--------------------+-------------------+
    |Review_ID|Rating|Year_Month|   Reviewer_Location|         Review_Text|             Branch|
    +---------+------+----------+--------------------+--------------------+-------------------+
    |670772142|     4|    2019-4|           Australia|If you've ever be...|Disneyland_HongKong|
    |670682799|     4|    2019-5|         Philippines|Its been a while ...|Disneyland_HongKong|
    |670623270|     4|    2019-4|United Arab Emirates|Thanks God it was...|Disneyland_HongKong|
    |670607911|     4|    2019-4|           Australia|HK Disneyland is ...|Disneyland_HongKong|
    |670607296|     4|    2019-4|      United Kingdom|the location is n...|Disneyland_HongKong|
    |670591897|     3|    2019-4|           Singapore|Have been to Disn...|Disneyland_HongKong|
    |670585330|     5|    2019-4|               India|Great place! Your...|Disneyland_HongKong|
    |670574142|     3|    2019-3|            Malaysia|Think of it as an...|Disneyland_HongKong|
    |670571027|     2|    2019-4|           Australia|Feel so let down ...|Disneyland_HongKong|
    |670570869|     5|    2019-3|               India|I can go on talki...|Disneyland_HongKong|
    |670443403|     5|    2019-4|       United States|Disneyland never ...|Disneyland_HongKong|
    |670435886|     5|    2019-4|              Canada|We spent the day ...|Disneyland_HongKong|
    |670376905|     4|    2019-4|           Australia|We spend two days...|Disneyland_HongKong|
    |670324965|     5|    2019-4|         Philippines|It was indeed the...|Disneyland_HongKong|
    |670274554|     5|    2018-9|           Australia|This place is HUG...|Disneyland_HongKong|
    |670205135|     3|    2019-1|      United Kingdom|We brought ticket...|Disneyland_HongKong|
    |670199487|     4|    2019-4|     Myanmar (Burma)|Its huge , not en...|Disneyland_HongKong|
    |670129921|     3|    2019-4|      United Kingdom|Around   60 per p...|Disneyland_HongKong|
    |670099231|     4|    2019-4|           Australia|It   s Disneyland...|Disneyland_HongKong|
    |670033848|     5|   2018-11|           Hong Kong|There is nothing ...|Disneyland_HongKong|
    +---------+------+----------+--------------------+--------------------+-------------------+
    only showing top 20 rows
    



```python
print((data.count(), len(data.columns)))
```

    (42656, 6)


##### Our dataset has 42,656 rows and 8 columns.

#### **Checking for NA values:**


```python
from pyspark.sql.functions import isnan, when, count, col

data.select([count(when(isnan(c), c)).alias(c) for c in data.columns]).show()
```

    +---------+------+----------+-----------------+-----------+------+
    |Review_ID|Rating|Year_Month|Reviewer_Location|Review_Text|Branch|
    +---------+------+----------+-----------------+-----------+------+
    |        0|     0|         0|                0|          0|     0|
    +---------+------+----------+-----------------+-----------+------+
    



```python
data.printSchema()
```

    root
     |-- Review_ID: integer (nullable = true)
     |-- Rating: integer (nullable = true)
     |-- Year_Month: string (nullable = true)
     |-- Reviewer_Location: string (nullable = true)
     |-- Review_Text: string (nullable = true)
     |-- Branch: string (nullable = true)
    


#### **changing date column to type datetime**


```python
from pyspark.sql.types import TimestampType 
from pyspark.sql.types import DateType 

data = data.withColumn('Year_Month', data['Year_Month'].cast(DateType()))
```


```python
data.printSchema()
```

    root
     |-- Review_ID: integer (nullable = true)
     |-- Rating: integer (nullable = true)
     |-- Year_Month: date (nullable = true)
     |-- Reviewer_Location: string (nullable = true)
     |-- Review_Text: string (nullable = true)
     |-- Branch: string (nullable = true)
    



```python
data.show()
```

    +---------+------+----------+--------------------+--------------------+-------------------+
    |Review_ID|Rating|Year_Month|   Reviewer_Location|         Review_Text|             Branch|
    +---------+------+----------+--------------------+--------------------+-------------------+
    |670772142|     4|2019-04-01|           Australia|If you've ever be...|Disneyland_HongKong|
    |670682799|     4|2019-05-01|         Philippines|Its been a while ...|Disneyland_HongKong|
    |670623270|     4|2019-04-01|United Arab Emirates|Thanks God it was...|Disneyland_HongKong|
    |670607911|     4|2019-04-01|           Australia|HK Disneyland is ...|Disneyland_HongKong|
    |670607296|     4|2019-04-01|      United Kingdom|the location is n...|Disneyland_HongKong|
    |670591897|     3|2019-04-01|           Singapore|Have been to Disn...|Disneyland_HongKong|
    |670585330|     5|2019-04-01|               India|Great place! Your...|Disneyland_HongKong|
    |670574142|     3|2019-03-01|            Malaysia|Think of it as an...|Disneyland_HongKong|
    |670571027|     2|2019-04-01|           Australia|Feel so let down ...|Disneyland_HongKong|
    |670570869|     5|2019-03-01|               India|I can go on talki...|Disneyland_HongKong|
    |670443403|     5|2019-04-01|       United States|Disneyland never ...|Disneyland_HongKong|
    |670435886|     5|2019-04-01|              Canada|We spent the day ...|Disneyland_HongKong|
    |670376905|     4|2019-04-01|           Australia|We spend two days...|Disneyland_HongKong|
    |670324965|     5|2019-04-01|         Philippines|It was indeed the...|Disneyland_HongKong|
    |670274554|     5|2018-09-01|           Australia|This place is HUG...|Disneyland_HongKong|
    |670205135|     3|2019-01-01|      United Kingdom|We brought ticket...|Disneyland_HongKong|
    |670199487|     4|2019-04-01|     Myanmar (Burma)|Its huge , not en...|Disneyland_HongKong|
    |670129921|     3|2019-04-01|      United Kingdom|Around   60 per p...|Disneyland_HongKong|
    |670099231|     4|2019-04-01|           Australia|It   s Disneyland...|Disneyland_HongKong|
    |670033848|     5|2018-11-01|           Hong Kong|There is nothing ...|Disneyland_HongKong|
    +---------+------+----------+--------------------+--------------------+-------------------+
    only showing top 20 rows
    



```python
from pyspark.sql import functions as F
data = data.withColumn('Month',F.from_unixtime(F.unix_timestamp(col('Year_Month'),'yyyy-MM-dd'),'MM'))
```


```python
data = data.withColumn('Year',F.from_unixtime(F.unix_timestamp(col('Year_Month'),'yyyy-MM-dd'),'yyyy'))
```


```python
data.show()
```

    +---------+------+----------+--------------------+--------------------+-------------------+-----+----+
    |Review_ID|Rating|Year_Month|   Reviewer_Location|         Review_Text|             Branch|Month|Year|
    +---------+------+----------+--------------------+--------------------+-------------------+-----+----+
    |670772142|     4|2019-04-01|           Australia|If you've ever be...|Disneyland_HongKong|   04|2019|
    |670682799|     4|2019-05-01|         Philippines|Its been a while ...|Disneyland_HongKong|   05|2019|
    |670623270|     4|2019-04-01|United Arab Emirates|Thanks God it was...|Disneyland_HongKong|   04|2019|
    |670607911|     4|2019-04-01|           Australia|HK Disneyland is ...|Disneyland_HongKong|   04|2019|
    |670607296|     4|2019-04-01|      United Kingdom|the location is n...|Disneyland_HongKong|   04|2019|
    |670591897|     3|2019-04-01|           Singapore|Have been to Disn...|Disneyland_HongKong|   04|2019|
    |670585330|     5|2019-04-01|               India|Great place! Your...|Disneyland_HongKong|   04|2019|
    |670574142|     3|2019-03-01|            Malaysia|Think of it as an...|Disneyland_HongKong|   03|2019|
    |670571027|     2|2019-04-01|           Australia|Feel so let down ...|Disneyland_HongKong|   04|2019|
    |670570869|     5|2019-03-01|               India|I can go on talki...|Disneyland_HongKong|   03|2019|
    |670443403|     5|2019-04-01|       United States|Disneyland never ...|Disneyland_HongKong|   04|2019|
    |670435886|     5|2019-04-01|              Canada|We spent the day ...|Disneyland_HongKong|   04|2019|
    |670376905|     4|2019-04-01|           Australia|We spend two days...|Disneyland_HongKong|   04|2019|
    |670324965|     5|2019-04-01|         Philippines|It was indeed the...|Disneyland_HongKong|   04|2019|
    |670274554|     5|2018-09-01|           Australia|This place is HUG...|Disneyland_HongKong|   09|2018|
    |670205135|     3|2019-01-01|      United Kingdom|We brought ticket...|Disneyland_HongKong|   01|2019|
    |670199487|     4|2019-04-01|     Myanmar (Burma)|Its huge , not en...|Disneyland_HongKong|   04|2019|
    |670129921|     3|2019-04-01|      United Kingdom|Around   60 per p...|Disneyland_HongKong|   04|2019|
    |670099231|     4|2019-04-01|           Australia|It   s Disneyland...|Disneyland_HongKong|   04|2019|
    |670033848|     5|2018-11-01|           Hong Kong|There is nothing ...|Disneyland_HongKong|   11|2018|
    +---------+------+----------+--------------------+--------------------+-------------------+-----+----+
    only showing top 20 rows
    



```python
from pyspark.sql.functions import min, max
min_date, max_date = data.select(min("Year_Month"), max("Year_Month")).first()
min_date, max_date
```




    (datetime.date(2010, 3, 1), datetime.date(2019, 5, 1))



##### The earliest review was in March 2010. The most recent review was in May 2019.

#### **Exploring some of the key columns:**

#### 1. Reviewer_Location


```python
data.select(F.countDistinct("Reviewer_Location")).show()
```

    +---------------------------------+
    |count(DISTINCT Reviewer_Location)|
    +---------------------------------+
    |                              162|
    +---------------------------------+
    


There are 162 unique reviewer locations.

#### The most popular reviewer locations:


```python
from pyspark.sql.functions import col
from pyspark.sql.functions import desc

rev_locs = data.groupBy('Reviewer_Location').count().sort(desc("count")).show()
```

    +--------------------+-----+
    |   Reviewer_Location|count|
    +--------------------+-----+
    |       United States|14551|
    |      United Kingdom| 9751|
    |           Australia| 4679|
    |              Canada| 2235|
    |               India| 1511|
    |         Philippines| 1070|
    |           Singapore| 1037|
    |         New Zealand|  756|
    |            Malaysia|  588|
    |           Hong Kong|  554|
    |           Indonesia|  530|
    |             Ireland|  487|
    |United Arab Emirates|  350|
    |         Netherlands|  253|
    |              France|  243|
    |        South Africa|  242|
    |            Thailand|  223|
    |             Germany|  194|
    |               China|  181|
    |               Spain|  147|
    +--------------------+-----+
    only showing top 20 rows
    


#### 2. Branch


```python
data.select(F.countDistinct("Branch")).show()
```

    +----------------------+
    |count(DISTINCT Branch)|
    +----------------------+
    |                     3|
    +----------------------+
    



```python
data.select("Branch").distinct().show()
```

    +--------------------+
    |              Branch|
    +--------------------+
    | Disneyland_HongKong|
    |    Disneyland_Paris|
    |Disneyland_Califo...|
    +--------------------+
    


There are 3 unique Branches.


```python
unique_branches = data.groupBy('Branch').count().sort(desc("count")).show()
```

    +--------------------+-----+
    |              Branch|count|
    +--------------------+-----+
    |Disneyland_Califo...|19406|
    |    Disneyland_Paris|13630|
    | Disneyland_HongKong| 9620|
    +--------------------+-----+
    



```python
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
```


```python
data.groupBy('Rating').count().sort(desc('count')).toPandas().plot(x='Rating',y='count',kind='bar')
display()
```


    
![png](final_project_files/final_project_33_0.png)
    



```python
data.groupBy('Branch').agg({'Rating':'avg'}).sort(desc('avg(Rating)')).show()
```

    +--------------------+-----------------+
    |              Branch|      avg(Rating)|
    +--------------------+-----------------+
    |Disneyland_Califo...|4.405338555086056|
    | Disneyland_HongKong|4.204158004158004|
    |    Disneyland_Paris| 3.96008804108584|
    +--------------------+-----------------+
    



```python
data.groupby('Branch').agg({"Rating":'avg'}).sort(desc('avg(Rating)')).toPandas().plot(x='Branch',y='avg(Rating)',kind='bar')
display()
```


    
![png](final_project_files/final_project_35_0.png)
    



```python
data.groupby('Branch', 'Rating').count().sort(desc('Rating')).show()
```

    +--------------------+------+-----+
    |              Branch|Rating|count|
    +--------------------+------+-----+
    |Disneyland_Califo...|     5|12518|
    | Disneyland_HongKong|     5| 4517|
    |    Disneyland_Paris|     5| 6111|
    |Disneyland_Califo...|     4| 3981|
    | Disneyland_HongKong|     4| 3230|
    |    Disneyland_Paris|     4| 3564|
    |Disneyland_Califo...|     3| 1661|
    |    Disneyland_Paris|     3| 2083|
    | Disneyland_HongKong|     3| 1365|
    |    Disneyland_Paris|     2| 1044|
    | Disneyland_HongKong|     2|  336|
    |Disneyland_Califo...|     2|  747|
    | Disneyland_HongKong|     1|  172|
    |Disneyland_Califo...|     1|  499|
    |    Disneyland_Paris|     1|  828|
    +--------------------+------+-----+
    



```python
Cal = data.filter(data['Branch'] == 'Disneyland_California')
Hkg = data.filter(data['Branch'] == 'Disneyland_HongKong')
Par = data.filter(data['Branch'] == 'Disneyland_Paris')
```


```python
cal_ratings = Cal.groupby('Month').agg({'Rating':'avg'}).sort(desc('Month'))
```


```python
cal_ratings.toPandas().plot(x='Month',y='avg(Rating)',kind='bar')
display()
```


    
![png](final_project_files/final_project_39_0.png)
    



```python
hkg_ratings = Hkg.groupby('Month').agg({'Rating':'avg'}).sort(desc('Month'))
```


```python
hkg_ratings.toPandas().plot(x='Month',y='avg(Rating)',kind='bar')
display()
```


    
![png](final_project_files/final_project_41_0.png)
    



```python
par_ratings = Par.groupby('Month').agg({'Rating':'avg'}).sort(desc('Month'))
```


```python
par_ratings.toPandas().plot(x='Month',y='avg(Rating)',kind='bar')
display()
```


    
![png](final_project_files/final_project_43_0.png)
    



```python
cal_visits = Cal.groupby('Month').count().sort(desc('Month'))
```


```python
plt.clf()
cal_visits.toPandas().plot(x='Month',y='count',kind='bar')
display()
```


    <Figure size 432x288 with 0 Axes>



    
![png](final_project_files/final_project_45_1.png)
    



```python
hkg_visits = Hkg.groupby('Month').count().sort(desc('Month'))
```


```python
hkg_visits.toPandas().plot(x='Month',y='count',kind='bar')
display()
```


    
![png](final_project_files/final_project_47_0.png)
    



```python
par_visits = Par.groupby('Month').count().sort(desc('Month'))
```


```python
par_visits.toPandas().plot(x='Month',y='count',kind='bar')
display()
```


    
![png](final_project_files/final_project_49_0.png)
    



```python
avg_review_length = data.toPandas().groupby('Rating').Review_Text.apply(lambda x: x.str.split().str.len().mean())
```


```python
avg_review_length.reset_index(name='mean_len_text').plot(x='Rating', y='mean_len_text',kind='bar')
display()
```


    
![png](final_project_files/final_project_51_0.png)
    



```python
review_len_branch = data.toPandas().groupby(['Branch','Rating']).Review_Text.apply(lambda x: x.str.split().str.len().mean())
```


```python
review_len_branch
```




    Branch                 Rating
    Disneyland_California  1         188.374749
                           2         176.285141
                           3         143.399759
                           4         128.714142
                           5         100.984343
    Disneyland_HongKong    1         137.418605
                           2         102.747024
                           3         100.022711
                           4         107.031579
                           5          98.147000
    Disneyland_Paris       1         214.875604
                           2         220.299808
                           3         199.429669
                           4         179.762907
                           5         138.195549
    Name: Review_Text, dtype: float64




```python
data.show(3)
```

    +---------+------+----------+--------------------+--------------------+-------------------+-----+----+
    |Review_ID|Rating|Year_Month|   Reviewer_Location|         Review_Text|             Branch|Month|Year|
    +---------+------+----------+--------------------+--------------------+-------------------+-----+----+
    |670772142|     4|2019-04-01|           Australia|If you've ever be...|Disneyland_HongKong|   04|2019|
    |670682799|     4|2019-05-01|         Philippines|Its been a while ...|Disneyland_HongKong|   05|2019|
    |670623270|     4|2019-04-01|United Arab Emirates|Thanks God it was...|Disneyland_HongKong|   04|2019|
    +---------+------+----------+--------------------+--------------------+-------------------+-----+----+
    only showing top 3 rows
    



```python
data.count()
```




    42656



#### Modeling:
- Binary classification of Ratings (good or bad)
  - Logistic Regression
  - Decision Tree
- Multilabel classification to predict the exact Rating of a Review_Text
  - Random Forest

#### Creating a new dataframe for feature engineering


```python
my_cols = data.select(['Review_ID','Rating','Review_Text'])
my_data = my_cols.dropna()
my_data.show(3)
```

    +---------+------+--------------------+
    |Review_ID|Rating|         Review_Text|
    +---------+------+--------------------+
    |670772142|     4|If you've ever be...|
    |670682799|     4|Its been a while ...|
    |670623270|     4|Thanks God it was...|
    +---------+------+--------------------+
    only showing top 3 rows
    


- Setting a threshold for Ratings: good = 1, bad = 0. 
- Let's assume that a Rating 3+ is good and a rating of 1 or 2 is bad.


```python
my_data = my_data.withColumn(
    'label',
    F.when((F.col("Rating") == 3), 1)\
    .when((F.col("Rating") == 4) , 1)\
    .when((F.col("Rating") == 5) , 1)\
    .otherwise(0)
)
my_data.show(3)
```

    +---------+------+--------------------+-----+
    |Review_ID|Rating|         Review_Text|label|
    +---------+------+--------------------+-----+
    |670772142|     4|If you've ever be...|    1|
    |670682799|     4|Its been a while ...|    1|
    |670623270|     4|Thanks God it was...|    1|
    +---------+------+--------------------+-----+
    only showing top 3 rows
    


#### Removing punctuations, etc.


```python
# Import the necessary functions
from pyspark.sql.functions import regexp_replace
from pyspark.ml.feature import Tokenizer

wrangled = my_data.withColumn('Review_Text', regexp_replace(my_data.Review_Text, '[_():;,.!?\\-]', " "))
wrangled = wrangled.withColumn('Review_Text', regexp_replace(wrangled.Review_Text, '[0-9]', " "))

# Merge multiple spaces
wrangled = wrangled.withColumn('Review_Text', regexp_replace(wrangled.Review_Text, ' +', ' '))

# Split the text into words
wrangled = Tokenizer(inputCol='Review_Text', outputCol="words").transform(wrangled)
 
wrangled.show(4, truncate=True)

```

    +---------+------+--------------------+-----+--------------------+
    |Review_ID|Rating|         Review_Text|label|               words|
    +---------+------+--------------------+-----+--------------------+
    |670772142|     4|If you've ever be...|    1|[if, you've, ever...|
    |670682799|     4|Its been a while ...|    1|[its, been, a, wh...|
    |670623270|     4|Thanks God it was...|    1|[thanks, god, it,...|
    |670607911|     4|HK Disneyland is ...|    1|[hk, disneyland, ...|
    +---------+------+--------------------+-----+--------------------+
    only showing top 4 rows
    


#### Removing stop words, etc.


```python
from pyspark.ml.feature import StopWordsRemover, HashingTF, IDF

# Remove stop words.
wrangled = StopWordsRemover(inputCol='words', outputCol='terms')\
      .transform(wrangled)
 
# Apply the hashing trick
wrangled = HashingTF(inputCol = 'terms', outputCol = 'hash', numFeatures=1024)\
      .transform(wrangled)

```


```python
wrangled.show(4, truncate=True)
```

    +---------+------+--------------------+-----+--------------------+--------------------+--------------------+
    |Review_ID|Rating|         Review_Text|label|               words|               terms|                hash|
    +---------+------+--------------------+-----+--------------------+--------------------+--------------------+
    |670772142|     4|If you've ever be...|    1|[if, you've, ever...|[ever, disneyland...|(1024,[26,47,93,1...|
    |670682799|     4|Its been a while ...|    1|[its, been, a, wh...|[since, d, last, ...|(1024,[4,27,47,62...|
    |670623270|     4|Thanks God it was...|    1|[thanks, god, it,...|[thanks, god, was...|(1024,[2,6,10,35,...|
    |670607911|     4|HK Disneyland is ...|    1|[hk, disneyland, ...|[hk, disneyland, ...|(1024,[43,71,83,9...|
    +---------+------+--------------------+-----+--------------------+--------------------+--------------------+
    only showing top 4 rows
    


#### TF-IDF


```python
# Convert hashed symbols to TF-IDF
tf_idf = IDF(inputCol = 'hash', outputCol = 'features')\
      .fit(wrangled).transform(wrangled)
      
tf_idf.select('features', 'label').show(3, truncate=True)
```

    +--------------------+-----+
    |            features|label|
    +--------------------+-----+
    |(1024,[26,47,93,1...|    1|
    |(1024,[4,27,47,62...|    1|
    |(1024,[2,6,10,35,...|    1|
    +--------------------+-----+
    only showing top 3 rows
    



```python
type(tf_idf)
```




    pyspark.sql.dataframe.DataFrame



Binary classification modeling


```python
from pyspark.ml.classification import LogisticRegression

# Split the data into training and testing sets
(train_df, test_df) = tf_idf.randomSplit([0.8,0.2], seed=13)


```


```python
train_df.show(3)
```

    +---------+------+--------------------+-----+--------------------+--------------------+--------------------+--------------------+
    |Review_ID|Rating|         Review_Text|label|               words|               terms|                hash|            features|
    +---------+------+--------------------+-----+--------------------+--------------------+--------------------+--------------------+
    |  3530053|     2|Obviously I haven...|    0|[obviously, i, ha...|[obviously, visit...|(1024,[7,38,40,59...|(1024,[7,38,40,59...|
    |  3998899|     4|Visited Hong Kong...|    1|[visited, hong, k...|[visited, hong, k...|(1024,[1,9,19,34,...|(1024,[1,9,19,34,...|
    |  4020946|     2|The park is small...|    0|[the, park, is, s...|[park, small, tin...|(1024,[1,7,10,18,...|(1024,[1,7,10,18,...|
    +---------+------+--------------------+-----+--------------------+--------------------+--------------------+--------------------+
    only showing top 3 rows
    



```python
test_df.show(3)
```

    +---------+------+--------------------+-----+--------------------+--------------------+--------------------+--------------------+
    |Review_ID|Rating|         Review_Text|label|               words|               terms|                hash|            features|
    +---------+------+--------------------+-----+--------------------+--------------------+--------------------+--------------------+
    |  3924467|     4|Visited Hkg Disne...|    1|[visited, hkg, di...|[visited, hkg, di...|(1024,[24,30,47,6...|(1024,[24,30,47,6...|
    |  4048615|     5|I went there on a...|    1|[i, went, there, ...|[went, weekday, c...|(1024,[8,30,33,40...|(1024,[8,30,33,40...|
    |  4602416|     4|We visited Hong K...|    1|[we, visited, hon...|[visited, hong, k...|(1024,[39,43,47,6...|(1024,[39,43,47,6...|
    +---------+------+--------------------+-----+--------------------+--------------------+--------------------+--------------------+
    only showing top 3 rows
    



```python
# Fit a Logistic Regression model to the training data
logistic = LogisticRegression(regParam=0.2).fit(train_df)
 
# Make predictions on the testing data
prediction = logistic.transform(test_df)
```

#### Model Evaluation


```python
# Create a confusion matrix, comparing predictions to known labels
prediction.groupBy('label', 'prediction').count().show()
```

    +-----+----------+-----+
    |label|prediction|count|
    +-----+----------+-----+
    |    1|       0.0|   37|
    |    0|       0.0|   91|
    |    1|       1.0| 7589|
    |    0|       1.0|  626|
    +-----+----------+-----+
    



```python
# Calculate the elements of the confusion matrix
TN = prediction.filter('prediction = 0 AND label = prediction').count()
TP = prediction.filter('prediction = 1 AND label = prediction').count()
FN = prediction.filter('prediction = 0 AND label = 1').count()
FP = prediction.filter('prediction = 1 AND label = 0').count()
```


```python
# Accuracy measures the proportion of correct predictions
accuracy = (TN +TP)/(TN + TP +FN +FP)
print(accuracy)
```

    0.920532182668105



```python
# Calculate precision and recall
precision = TP/(TP+FP)
recall = TP/(TP+FN)
print('precision = {:.2f}\nrecall    = {:.2f}'.format(precision, recall))
```

    precision = 0.92
    recall    = 1.00



```python
# Find weighted precision
from pyspark.ml.evaluation import BinaryClassificationEvaluator

binary_evaluator =  BinaryClassificationEvaluator()

# Find AUC
auc = binary_evaluator.evaluate(prediction, {binary_evaluator.metricName: "areaUnderROC"})
print(auc)
```

    0.9198899675594138


#### Pipeline for Binary Classification, using Logistic Regression


```python
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
 
# Break text into tokens at non-word characters
tokenizer = Tokenizer(inputCol='Review_Text', outputCol='words')
 
# Remove stop words
remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol='terms')
 
# Apply the hashing trick and transform to TF-IDF
hasher = HashingTF(inputCol=remover.getOutputCol(), outputCol="hash")
idf = IDF(inputCol=hasher.getOutputCol(), outputCol="features")
 
# Create a logistic regression object and add everything to a pipeline
logistic = LogisticRegression()
pipeline = Pipeline(stages=[tokenizer, remover, hasher, idf, logistic])

```


```python
my_data.show(3)
```

    +---------+------+--------------------+-----+
    |Review_ID|Rating|         Review_Text|label|
    +---------+------+--------------------+-----+
    |670772142|     4|If you've ever be...|    1|
    |670682799|     4|Its been a while ...|    1|
    |670623270|     4|Thanks God it was...|    1|
    +---------+------+--------------------+-----+
    only showing top 3 rows
    



```python
# create dataframe for Binary Classification pipeline
new_cols = my_data[['Review_Text','label']]
new_df = new_cols.dropna()
new_df.show(3)

# Split the data into training and testing sets
(pipeline_train, pipeline_test) = new_df.randomSplit([0.8,0.2], seed=13)
```

    +--------------------+-----+
    |         Review_Text|label|
    +--------------------+-----+
    |If you've ever be...|    1|
    |Its been a while ...|    1|
    |Thanks God it was...|    1|
    +--------------------+-----+
    only showing top 3 rows
    



```python
# Train the pipeline on the training data
pipeline = pipeline.fit(pipeline_train)
 
# Make predictions on the testing data
predictions = pipeline.transform(pipeline_test)
```


```python
evaluator = BinaryClassificationEvaluator()

evaluator.evaluate(predictions)
```




    0.8092143791584008




```python
# Create a confusion matrix, comparing predictions to known labels
predictions.groupBy('label', 'prediction').count().show()
```

    +-----+----------+-----+
    |label|prediction|count|
    +-----+----------+-----+
    |    1|       0.0|  273|
    |    0|       0.0|  254|
    |    1|       1.0| 7356|
    |    0|       1.0|  460|
    +-----+----------+-----+
    



```python
# Calculate the elements of the confusion matrix
TN = predictions.filter('prediction = 0 AND label = prediction').count()
TP = predictions.filter('prediction = 1 AND label = prediction').count()
FN = predictions.filter('prediction = 0 AND label = 1').count()
FP = predictions.filter('prediction = 1 AND label = 0').count()
```


```python
# Calculate precision and recall
precision = TP/(TP+FP)
recall = TP/(TP+FN)
print('precision = {:.2f}\nrecall    = {:.2f}'.format(precision, recall))
```

    precision = 0.94
    recall    = 0.96


- Note: Interesting how drop in performance could be because punctuations weren't removed.

#### Pipeline for Binary Classification, using Decision Trees


```python
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml import Pipeline
 
# Break text into tokens at non-word characters
tokenizer1 = Tokenizer(inputCol='Review_Text', outputCol='words')
 
# Remove stop words
remover1 = StopWordsRemover(inputCol=tokenizer1.getOutputCol(), outputCol='terms')
 
# Apply the hashing trick and transform to TF-IDF
hasher1 = HashingTF(inputCol=remover1.getOutputCol(), outputCol="hash")
idf1 = IDF(inputCol=hasher1.getOutputCol(), outputCol="features")
 
# Create a decision tree object and add everything to a pipeline
tree = DecisionTreeClassifier()
pipeline2 = Pipeline(stages=[tokenizer1, remover1, hasher1, idf1, tree])
```

#### Training the model and making predictions


```python
# Train the pipeline on the training data
pipeline2 = pipeline2.fit(pipeline_train)
 
# Make predictions on the testing data
predictions2 = pipeline2.transform(pipeline_test)
```

#### Model Evaluation


```python
evaluator = BinaryClassificationEvaluator()

evaluator.evaluate(predictions2)
```




    0.6368038367529475




```python
# Create a confusion matrix, comparing predictions to known labels
predictions2.groupBy('label', 'prediction').count().show()
```

    +-----+----------+-----+
    |label|prediction|count|
    +-----+----------+-----+
    |    1|       0.0|   36|
    |    0|       0.0|   35|
    |    1|       1.0| 7593|
    |    0|       1.0|  679|
    +-----+----------+-----+
    



```python
# Calculate the elements of the confusion matrix
TN_1 = predictions2.filter('prediction = 0 AND label = prediction').count()
TP_1 = predictions2.filter('prediction = 1 AND label = prediction').count()
FN_1 = predictions2.filter('prediction = 0 AND label = 1').count()
FP_1 = predictions2.filter('prediction = 1 AND label = 0').count()
 
# Accuracy measures the proportion of correct predictions
accuracy_1 = (TN_1 +TP_1)/(TN_1 + TP_1 +FN_1 +FP_1)
print(accuracy_1)
```

    0.9142994126812897



```python
# Calculate precision and recall
precision = TP_1/(TP_1+FP_1)
recall = TP_1/(TP_1+FN_1)
print('precision = {:.2f}\nrecall    = {:.2f}'.format(precision, recall))
```

    precision = 0.92
    recall    = 1.00


### Modeling to predict exact Ratings of reviews


```python
data.show(3)
```

    +---------+------+----------+--------------------+--------------------+-------------------+-----+----+
    |Review_ID|Rating|Year_Month|   Reviewer_Location|         Review_Text|             Branch|Month|Year|
    +---------+------+----------+--------------------+--------------------+-------------------+-----+----+
    |670772142|     4|2019-04-01|           Australia|If you've ever be...|Disneyland_HongKong|   04|2019|
    |670682799|     4|2019-05-01|         Philippines|Its been a while ...|Disneyland_HongKong|   05|2019|
    |670623270|     4|2019-04-01|United Arab Emirates|Thanks God it was...|Disneyland_HongKong|   04|2019|
    +---------+------+----------+--------------------+--------------------+-------------------+-----+----+
    only showing top 3 rows
    


#### Creating dataframe with relevant columns


```python
my_cols2 = data.select(['Rating','Review_Text'])
my_data2 = my_cols2.dropna()
my_data2.show(10)
```

    +------+--------------------+
    |Rating|         Review_Text|
    +------+--------------------+
    |     4|If you've ever be...|
    |     4|Its been a while ...|
    |     4|Thanks God it was...|
    |     4|HK Disneyland is ...|
    |     4|the location is n...|
    |     3|Have been to Disn...|
    |     5|Great place! Your...|
    |     3|Think of it as an...|
    |     2|Feel so let down ...|
    |     5|I can go on talki...|
    +------+--------------------+
    only showing top 10 rows
    



```python
my_data2 = my_data2.withColumn(
    'label',
    F.when((F.col("Rating") == 5), 5)\
    .when((F.col("Rating") == 4) , 4)\
    .when((F.col("Rating") == 3) , 3)\
    .when((F.col("Rating") == 2) , 2)\
    .when((F.col("Rating") == 1) , 1)\
    .otherwise(0)
)
my_data2.show(10)
```

    +------+--------------------+-----+
    |Rating|         Review_Text|label|
    +------+--------------------+-----+
    |     4|If you've ever be...|    4|
    |     4|Its been a while ...|    4|
    |     4|Thanks God it was...|    4|
    |     4|HK Disneyland is ...|    4|
    |     4|the location is n...|    4|
    |     3|Have been to Disn...|    3|
    |     5|Great place! Your...|    5|
    |     3|Think of it as an...|    3|
    |     2|Feel so let down ...|    2|
    |     5|I can go on talki...|    5|
    +------+--------------------+-----+
    only showing top 10 rows
    



```python

```


```python

```


```python
my_data2.count()
```




    42656




```python
wrangled.show()
```

    +---------+------+--------------------+-----+--------------------+--------------------+--------------------+
    |Review_ID|Rating|         Review_Text|label|               words|               terms|                hash|
    +---------+------+--------------------+-----+--------------------+--------------------+--------------------+
    |670772142|     4|If you've ever be...|    1|[if, you've, ever...|[ever, disneyland...|(1024,[26,47,93,1...|
    |670682799|     4|Its been a while ...|    1|[its, been, a, wh...|[since, d, last, ...|(1024,[4,27,47,62...|
    |670623270|     4|Thanks God it was...|    1|[thanks, god, it,...|[thanks, god, was...|(1024,[2,6,10,35,...|
    |670607911|     4|HK Disneyland is ...|    1|[hk, disneyland, ...|[hk, disneyland, ...|(1024,[43,71,83,9...|
    |670607296|     4|the location is n...|    1|[the, location, i...|[location, city, ...|(1024,[3,36,87,93...|
    |670591897|     3|Have been to Disn...|    1|[have, been, to, ...|[disney, world, d...|(1024,[6,8,9,35,5...|
    |670585330|     5|Great place Your ...|    1|[great, place, yo...|[great, place, da...|(1024,[109,157,19...|
    |670574142|     3|Think of it as an...|    1|[think, of, it, a...|[think, intro, di...|(1024,[49,60,87,1...|
    |670571027|     2|Feel so let down ...|    0|[feel, so, let, d...|[feel, let, place...|(1024,[107,118,14...|
    |670570869|     5|I can go on talki...|    1|[i, can, go, on, ...|[go, talking, dis...|(1024,[6,23,31,36...|
    |670443403|     5|Disneyland never ...|    1|[disneyland, neve...|[disneyland, neve...|(1024,[7,9,264,33...|
    |670435886|     5|We spent the day ...|    1|[we, spent, the, ...|[spent, day, grow...|(1024,[9,40,57,62...|
    |670376905|     4|We spend two days...|    1|[we, spend, two, ...|[spend, two, days...|(1024,[1,44,93,10...|
    |670324965|     5|It was indeed the...|    1|[it, was, indeed,...|[indeed, happiest...|(1024,[284,319,34...|
    |670274554|     5|This place is HUG...|    1|[this, place, is,...|[place, huge, def...|(1024,[91,109,134...|
    |670205135|     3|We brought ticket...|    1|[we, brought, tic...|[brought, tickets...|(1024,[16,47,69,7...|
    |670199487|     4|Its huge not enou...|    1|[its, huge, not, ...|[huge, enough, vi...|(1024,[31,109,122...|
    |670129921|     3|Around per person...|    1|[around, per, per...|[around, per, per...|(1024,[109,127,19...|
    |670099231|     4|It s Disneyland I...|    1|[it, s, disneylan...|[disneyland, need...|(1024,[217,286,32...|
    |670033848|     5|There is nothing ...|    1|[there, is, nothi...|[nothing, say, ex...|(1024,[47,66,83,1...|
    +---------+------+--------------------+-----+--------------------+--------------------+--------------------+
    only showing top 20 rows
    



```python
wrangled = wrangled.withColumn(
    'label',
    F.when((F.col("Rating") == 5), 5)\
    .when((F.col("Rating") == 4) , 4)\
    .when((F.col("Rating") == 3) , 3)\
    .when((F.col("Rating") == 2) , 2)\
    .when((F.col("Rating") == 1) , 1)\
    .otherwise(0)
)
wrangled.show(10)
```

    +---------+------+--------------------+-----+--------------------+--------------------+--------------------+
    |Review_ID|Rating|         Review_Text|label|               words|               terms|                hash|
    +---------+------+--------------------+-----+--------------------+--------------------+--------------------+
    |670772142|     4|If you've ever be...|    4|[if, you've, ever...|[ever, disneyland...|(1024,[26,47,93,1...|
    |670682799|     4|Its been a while ...|    4|[its, been, a, wh...|[since, d, last, ...|(1024,[4,27,47,62...|
    |670623270|     4|Thanks God it was...|    4|[thanks, god, it,...|[thanks, god, was...|(1024,[2,6,10,35,...|
    |670607911|     4|HK Disneyland is ...|    4|[hk, disneyland, ...|[hk, disneyland, ...|(1024,[43,71,83,9...|
    |670607296|     4|the location is n...|    4|[the, location, i...|[location, city, ...|(1024,[3,36,87,93...|
    |670591897|     3|Have been to Disn...|    3|[have, been, to, ...|[disney, world, d...|(1024,[6,8,9,35,5...|
    |670585330|     5|Great place Your ...|    5|[great, place, yo...|[great, place, da...|(1024,[109,157,19...|
    |670574142|     3|Think of it as an...|    3|[think, of, it, a...|[think, intro, di...|(1024,[49,60,87,1...|
    |670571027|     2|Feel so let down ...|    2|[feel, so, let, d...|[feel, let, place...|(1024,[107,118,14...|
    |670570869|     5|I can go on talki...|    5|[i, can, go, on, ...|[go, talking, dis...|(1024,[6,23,31,36...|
    +---------+------+--------------------+-----+--------------------+--------------------+--------------------+
    only showing top 10 rows
    



```python
wrangled.printSchema()
```

    root
     |-- Review_ID: integer (nullable = true)
     |-- Rating: integer (nullable = true)
     |-- Review_Text: string (nullable = true)
     |-- label: integer (nullable = false)
     |-- words: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- terms: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- hash: vector (nullable = true)
    



```python
tf_idf4 = IDF(inputCol = 'hash', outputCol = 'features')\
      .fit(wrangled).transform(wrangled)
tf_idf4.show()
```

    +---------+------+--------------------+-----+--------------------+--------------------+--------------------+--------------------+
    |Review_ID|Rating|         Review_Text|label|               words|               terms|                hash|            features|
    +---------+------+--------------------+-----+--------------------+--------------------+--------------------+--------------------+
    |670772142|     4|If you've ever be...|    4|[if, you've, ever...|[ever, disneyland...|(1024,[26,47,93,1...|(1024,[26,47,93,1...|
    |670682799|     4|Its been a while ...|    4|[its, been, a, wh...|[since, d, last, ...|(1024,[4,27,47,62...|(1024,[4,27,47,62...|
    |670623270|     4|Thanks God it was...|    4|[thanks, god, it,...|[thanks, god, was...|(1024,[2,6,10,35,...|(1024,[2,6,10,35,...|
    |670607911|     4|HK Disneyland is ...|    4|[hk, disneyland, ...|[hk, disneyland, ...|(1024,[43,71,83,9...|(1024,[43,71,83,9...|
    |670607296|     4|the location is n...|    4|[the, location, i...|[location, city, ...|(1024,[3,36,87,93...|(1024,[3,36,87,93...|
    |670591897|     3|Have been to Disn...|    3|[have, been, to, ...|[disney, world, d...|(1024,[6,8,9,35,5...|(1024,[6,8,9,35,5...|
    |670585330|     5|Great place Your ...|    5|[great, place, yo...|[great, place, da...|(1024,[109,157,19...|(1024,[109,157,19...|
    |670574142|     3|Think of it as an...|    3|[think, of, it, a...|[think, intro, di...|(1024,[49,60,87,1...|(1024,[49,60,87,1...|
    |670571027|     2|Feel so let down ...|    2|[feel, so, let, d...|[feel, let, place...|(1024,[107,118,14...|(1024,[107,118,14...|
    |670570869|     5|I can go on talki...|    5|[i, can, go, on, ...|[go, talking, dis...|(1024,[6,23,31,36...|(1024,[6,23,31,36...|
    |670443403|     5|Disneyland never ...|    5|[disneyland, neve...|[disneyland, neve...|(1024,[7,9,264,33...|(1024,[7,9,264,33...|
    |670435886|     5|We spent the day ...|    5|[we, spent, the, ...|[spent, day, grow...|(1024,[9,40,57,62...|(1024,[9,40,57,62...|
    |670376905|     4|We spend two days...|    4|[we, spend, two, ...|[spend, two, days...|(1024,[1,44,93,10...|(1024,[1,44,93,10...|
    |670324965|     5|It was indeed the...|    5|[it, was, indeed,...|[indeed, happiest...|(1024,[284,319,34...|(1024,[284,319,34...|
    |670274554|     5|This place is HUG...|    5|[this, place, is,...|[place, huge, def...|(1024,[91,109,134...|(1024,[91,109,134...|
    |670205135|     3|We brought ticket...|    3|[we, brought, tic...|[brought, tickets...|(1024,[16,47,69,7...|(1024,[16,47,69,7...|
    |670199487|     4|Its huge not enou...|    4|[its, huge, not, ...|[huge, enough, vi...|(1024,[31,109,122...|(1024,[31,109,122...|
    |670129921|     3|Around per person...|    3|[around, per, per...|[around, per, per...|(1024,[109,127,19...|(1024,[109,127,19...|
    |670099231|     4|It s Disneyland I...|    4|[it, s, disneylan...|[disneyland, need...|(1024,[217,286,32...|(1024,[217,286,32...|
    |670033848|     5|There is nothing ...|    5|[there, is, nothi...|[nothing, say, ex...|(1024,[47,66,83,1...|(1024,[47,66,83,1...|
    +---------+------+--------------------+-----+--------------------+--------------------+--------------------+--------------------+
    only showing top 20 rows
    



```python
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Index labels, adding metadata to the label column.
# Fit on whole dataset to include all labels in index.
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(tf_idf4)

# Automatically identify categorical features, and index them.

featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=5).fit(tf_idf4)

# Split the data into training and test sets (20% held out for testing)
(trainingData, testData) = tf_idf4.randomSplit([0.8, 0.2])

# Train a RandomForest model.
rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", numTrees=50)

# Convert indexed labels back to original labels.
labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                               labels=labelIndexer.labels)

# Chain indexers and forest in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, rf, labelConverter])

# Train model.  This also runs the indexers.
model = pipeline.fit(trainingData)
```


```python
# Make predictions.
predictions = model.transform(testData)
```


```python
# Select example rows to display.
predictions.select("predictedLabel", "label", "features", "Review_Text").show(20, truncate=False)
```

    +--------------+-----+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |predictedLabel|label|features                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |Review_Text                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
    +--------------+-----+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |5             |4    |(1024,[24,30,47,62,87,95,109,113,120,122,123,124,126,127,133,139,159,180,191,204,205,214,217,241,242,264,281,284,287,289,290,298,305,308,319,330,331,343,346,355,367,377,407,426,434,455,479,496,510,559,565,579,590,600,612,636,649,656,685,710,729,734,768,786,810,818,822,825,839,853,869,870,876,942,948,956,968,977,986],[3.3727022650443597,3.293238093690113,3.845063537793471,3.3976170485876476,2.0381329927845626,5.121570438255753,0.9121265935404949,4.542849468023136,3.674380206658057,1.5904432392761008,4.733294114414498,2.9172434078907306,3.189014587819362,2.0742274119996362,3.412442593693874,1.945558568698002,2.5101901957889328,2.934734015556955,3.811880383431027,6.786842476472625,3.5478045573573964,2.2906306705090067,1.9193307418356567,1.8693086289784864,1.897674952051541,1.8942406683139688,4.540649247113533,2.4341057756559064,3.0555543012495496,2.312882437656218,2.841310363696892,3.594479695927526,3.483927900154584,1.4451213638577318,1.2349305576216125,1.6610804423018484,2.82615855867629,1.8767844437940073,1.5032689466716074,2.726433202182221,2.9821576678653305,3.291345945538075,2.5937976261543785,2.953884010694011,3.556802573076957,2.477270083443827,3.4110211293464965,1.2790855068534763,2.034181838218956,1.499901585813179,2.5805495103719243,1.7550954848564635,1.9143893115209807,2.6881358819430803,2.531077023252643,3.2044921108882747,2.030603376715591,3.7005989369631767,0.8633753391199054,3.11291769612947,4.072793527844644,3.4486521975641433,2.5502190830899947,7.433718915669914,3.002247107796185,1.1328073227327098,2.093821105900038,2.3133562590344265,2.9789251552376097,1.980784646370107,3.414578585962023,3.252416099169858,2.980309238503548,1.8680933774237913,2.962463878183538,3.173772971850745,2.5629119098884137,2.9937884107453363,2.8318301085967423])                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |Visited Hkg Disney Sept days after it officially opens to the public Hkg disney is small you can tour the area minus the rides in an hour or so There are main attractions inside namely Mainstreet USA where you can have the opportunity to see and take pictures of your favorite disney characters lots of souvenir shops where you can buy almost anything you could imagine with that famous disney logo The Adventureland Fantasyland and Tomorrowland offers fun and rides Not to miss out the musical the Golden Mickey at Disney and the Festival of Lion King where you can experience one of a kind entertainment at its best The only setback is that it took us mins queue time to get on a ride considering its a weekday so expect the crowd during Sat & Sun My daughter loves Winnie the Pooh I have to ride along in Pooh's hunny pot twice for the adventure of Pooh so we have to line up again But its all worth the wait It brings out the kid in me Have a magical day                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
    |5             |2    |(1024,[47,49,80,146,164,175,185,195,210,215,251,261,262,269,342,409,432,434,479,496,506,578,590,625,630,649,653,686,721,777,839,847,863,868,879,965,986],[3.845063537793471,3.1912924931323556,4.608857497140067,1.346787476821377,3.4238876399397467,3.8708494305505794,1.8537740431477823,1.154063593204324,2.676824707361557,1.6262700632181895,2.9928529569820785,2.181663047721467,2.564738394414448,4.013258292501154,5.151229615132843,2.5739211953967827,2.2197709610721628,3.556802573076957,3.4110211293464965,1.2790855068534763,2.888615090894871,2.400712374457187,1.9143893115209807,4.159656995524095,3.284438539738264,2.030603376715591,3.6716553914388466,5.511010034008396,2.5887913578762345,2.9246395695161986,2.9789251552376097,2.337338223720912,1.2680347685391393,2.3724122066505666,1.6908960042374817,3.2343975936671794,1.4159150542983712])                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |Visited this place last Jan Our all ride fee of USD w as not worth it Too small with very few rides for children and especially for adults I think there were more commercial stores or souvenir shops than the rides you can enjoy They have to drop the entrance fee by at least half so people can appreciate the place and what they paid for NOt worth visiting the place at the moment not until they upgrade it very soon The Ocean park is a lot better theme park to go rather than this place                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
    |5             |3    |(1024,[131,185,200,202,330,408,425,455,551,590,632,717,726,750,822,890,957,987,1006],[1.623056731086993,1.8537740431477823,2.232147625410842,2.4472939630344857,1.6610804423018484,2.326475111463541,2.851405341411074,2.477270083443827,2.7379607073532886,1.9143893115209807,2.852623615673428,3.7472093154048,3.2566675480272163,1.1889345770687514,2.093821105900038,2.3904215710094134,0.7512782544532749,1.8576726835634396,3.9741833292697395])                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |After going to disneyworld in orlando a few years ago This disneyland felt small but if your going to Hong Kong anyway and you have children this is great my year old daughter enjoyed it alot Brett                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
    |5             |1    |(1024,[9,22,31,44,83,109,127,145,146,153,164,178,182,193,195,197,215,233,250,256,257,273,276,302,308,312,315,319,322,350,386,395,413,419,429,452,458,484,495,496,535,555,590,598,612,618,632,633,637,662,688,697,699,700,704,709,710,729,731,743,748,773,776,805,816,819,840,847,856,858,871,886,919,924,927,956,957,979,1004,1018],[4.5300351242825725,3.6836653244337367,2.208612046996743,2.2424694475874043,6.4101399571453195,1.8242531870809897,2.0742274119996362,4.255718208033643,2.693574953642754,2.6809230737538394,3.4238876399397467,5.0459324232246905,2.9611042586674983,2.9017592275566884,1.154063593204324,1.7827280079722563,1.6262700632181895,3.1698590725296083,4.752863727895553,5.981969341181815,1.891439546034257,3.1472374182247793,3.50555036416775,3.470270631732277,1.4451213638577318,5.5984391764810075,3.2120305635202837,3.704791672864838,2.4899124768590037,2.4964363973174417,3.085881810582195,3.3530738853007778,1.494244751118131,3.1914953595159443,4.108438779029894,3.908676289922742,3.9359130238976414,3.240966742402649,2.226265896222711,0.6395427534267382,5.847028193567205,5.997898214325182,1.9143893115209807,4.017463106382475,1.2655385116263216,1.571080447556176,2.852623615673428,2.3589288563132795,2.2591643261595746,4.021070832237948,1.8000219363454435,3.0348639079921034,1.9503271381221863,3.052572191683701,1.6335680557696795,1.8414291254595958,9.33875308838841,2.4436761167067864,1.4994814618704382,2.687791232620351,2.536796062757854,2.8522173593200852,3.881024758592232,3.085362014506692,2.917881861756718,3.6195350022696733,2.6778477253535917,2.337338223720912,4.035651836100913,3.0280611606693513,4.937859503055217,3.27676540313044,2.004861475781628,2.3794758081693166,2.332012624108955,3.173772971850745,2.2538347633598246,3.213778306464444,4.153668953679472,3.9371142252432754])                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |Was there a month ago with my three kids what a dissapointment My kids' favourite rides in Disneyland Magic Kingdom is Haunted Mansion Big Thunder Mountain and Pirates of the Carribean Guess what They are not in this so called Disneyland Totally ruined their expectation Ended up leaving the park early and decided to never return We got a second day free since we were staing at the Hollywood hotel kids does not want to return for a second day and we ended up in the hotel's pool instead If you have been to other disney parks dont bother with HK disneyland If you have never been to one save up and go to a REAL disney park like the one in the US or Japan Even the Paris park is lightyears ahead of this one Missing Big Thunder Mountain Splash Mountain Haunted Mansion Small world Star Tours Peter Pan Tom Sawyer Snow White Fantasmic and many more oh also no evening parade Disney must think people in HK just like to walk around aimlessly in the park and not want to be in any attraction Sad                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
    |5             |4    |(1024,[2,19,20,56,59,62,83,87,88,93,95,109,122,123,127,131,132,146,156,158,167,168,181,188,195,197,204,207,213,215,216,226,232,253,256,257,264,280,295,308,319,325,338,339,340,346,356,360,364,365,367,369,376,385,407,409,413,415,418,419,441,444,455,459,462,476,480,488,495,496,509,510,512,513,521,527,550,561,567,579,587,590,591,596,601,608,612,616,618,625,629,630,654,657,660,662,688,690,699,704,725,728,729,731,732,737,741,775,777,778,803,806,807,818,829,843,847,857,863,871,872,879,883,919,927,941,942,957,962,976,981,982,986,988,992,995,996,998,1005,1006,1019,1020,1023],[3.7960988880936237,3.141254261947945,4.409042782898595,2.9142137583108623,3.0098264903617844,3.3976170485876476,22.43548985000862,2.0381329927845626,2.518010655632218,2.6172833136705407,2.5607852191278764,0.9121265935404949,1.5904432392761008,2.366647057207249,2.0742274119996362,1.623056731086993,3.271382712386849,5.387149907285508,4.275752267066759,3.6125602573426017,3.1219196102404894,3.742251447044013,3.072622988729262,3.0436788524361376,2.308127186408648,8.913640039861281,3.3934212382363125,3.668850238648596,4.196358362374523,4.878810189654568,5.223677889476157,3.400424067974632,3.0436788524361376,1.7135307154143558,2.9909846705909073,1.891439546034257,7.576962673255875,3.3631783835331044,1.842464398790249,2.8902427277154636,1.2349305576216125,4.361664965779335,5.443575696215375,3.8585519027401736,10.16772887753135,1.5032689466716074,8.124875274899937,3.7602160020193116,1.8909734542057308,5.235208991245752,2.9821576678653305,1.6757518480731843,3.3457282763115095,2.5856751197670262,5.187595252308757,2.5739211953967827,1.494244751118131,4.1793695367880535,3.9738380581979698,1.5957476797579722,5.444288835820007,2.9998902837026544,2.477270083443827,2.608968587162185,3.5961876382726823,3.034376459773824,5.2905781030142895,4.648629125844975,2.226265896222711,0.6395427534267382,4.011962116039708,6.1025455146568675,3.9028521616367535,5.303831961989507,3.8798890401283046,3.141796708394661,1.9821449594518337,3.1277878586089214,1.9912040761879621,1.7550954848564635,4.164171675878621,1.9143893115209807,2.741590475403867,2.792309771880317,3.6958663204630775,1.8166103333155523,1.2655385116263216,2.774113667109427,1.571080447556176,12.478970986572286,2.0343610978770497,3.284438539738264,2.6010383314862078,3.988913720603417,2.034719713624124,4.021070832237948,1.8000219363454435,5.818942696086055,3.9006542762443726,3.267136111539359,7.42601919489903,2.5881673328949857,11.403821877965004,4.4984443856113145,3.9323180529797823,3.8913046892119816,5.197316525253572,4.187055969712209,2.9246395695161986,3.1003455032959275,2.739048255040687,6.881145658681069,2.9453771315442756,2.2656146454654196,3.268299145342861,2.6380497964630267,2.337338223720912,3.7352760771358624,1.2680347685391393,2.4689297515276083,4.444340564979619,3.3817920084749633,8.118669994699728,2.004861475781628,2.332012624108955,2.896650659613966,3.7361867548475827,4.507669526719649,4.755584818009914,3.416005119727477,3.6021885135458196,3.2207999854017957,1.4159150542983712,3.1367536596456183,4.654593506462752,3.525708640970664,4.983220540272195,2.638705749257947,2.1560285055238593,5.96127499390461,3.595333302466767,2.1775171048010558,2.9471620494657293])                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |My family has been going to Disney parks since my daughter was and she is now this year I have been to every Disney park except for Disney Paris so I can probably give a bad review to the HK DisneyLand However I won't because of my experiences and my expectations before going there I have heard many reviews about how small the park was and how long the lines were and were told by friends and family in HK not to go and instead go to Ocean Park but I also suspected that many of these people have not yet experienced the HK DisneyLand yet On the day we went to HK DisneyLand the weather was no different than any of the other days we were in HK during the week hot and humid Based on my research I had expected the park to be much more smaller than all the other Disney parks and probably very crowded With that in mind we went to check out if it was a true Disney park and I feel it passed the test Although much smaller Disney paid attention to make sure the layout was true to the ones in America In fact I think some of the shows were better such as the parade Lion King show and the Mickey Awards show We bought some souvenirs pins and found the prices to be cheaper than that of Florida and California As a Chinese American who moved to the US very young and grew up there I found it to be a blast to hear Mickey and Minnie speak Cantonese While there are problems with Mainlanders behavior there were sufficient cast members to handle situations One such example was when we were waiting in line for the Pooh ride and some people tried to cut in line a cast member immediately caught them and force them out of the line As a comparison we went to Ocean Park earlier in the week and although bigger in size the people were ruder and lines were longer Ocean Park truly did not have enough staff to handle the crowd My final thoughts are if you are a true Disney fan and have been to other Disney parks be open minded and realize that although Disney may have made some miscalculations you are going to HK DisneyLand to experience the magic of Disney Long lines and bad weather are things to expect and in most cases are not controllable For first timers to Disney parks don't fret and get discouraged If you are able to follow up to the HK Disneyland with the Japan Disney parks Paris Disney park then to Disneyland in California and the ultimate treasure Disney World in Florida You will be amazed and it will only get better                                                                                                                                                                   |
    |5             |4    |(1024,[6,10,43,73,109,116,131,142,146,153,174,180,217,225,236,243,263,290,319,341,342,346,389,393,407,413,423,454,461,476,478,488,493,496,527,557,559,565,591,602,603,618,660,675,689,699,709,715,727,728,729,731,748,750,758,761,768,773,775,794,811,816,818,822,832,835,839,853,863,890,927,931,942,957,983,984,995,1006],[3.5126009221644168,2.1914742108596585,2.7343440668831,2.5947390980582203,2.7363797806214847,3.4117316089500953,1.623056731086993,3.2213873569311646,2.693574953642754,2.6809230737538394,1.7088590225160791,2.934734015556955,3.8386614836713133,2.3055667708108505,3.713009597449515,2.8735646395797834,2.592543707494785,5.682620727393784,2.469861115243225,7.2976627435162085,1.2878074037832108,1.5032689466716074,2.2913258391153826,3.172653150905056,2.5937976261543785,2.988489502236262,3.595333302466767,2.30204605364284,3.0471279812558554,3.034376459773824,2.8345035306084707,1.549543041948325,3.4915966493938844,1.2790855068534763,3.141796708394661,1.496964418066451,1.499901585813179,5.1610990207438485,2.741590475403867,1.5117372940925327,3.7940133816026025,1.571080447556176,4.069439427248248,3.6073609388708077,2.5937976261543785,1.9503271381221863,3.6828582509191916,3.3503965075300615,4.333009882335289,5.176334665789971,0.8145587055689288,2.9989629237408764,2.536796062757854,2.3778691541375028,3.7703375459173176,1.8961124517336496,2.5502190830899947,2.8522173593200852,4.187055969712209,2.953434471464144,2.5185923892146502,1.458940930878359,2.2656146454654196,2.093821105900038,2.226265896222711,4.874638980518005,2.9789251552376097,1.980784646370107,2.5360695370782786,2.3904215710094134,4.66402524821791,2.1841588892832857,1.8680933774237913,2.2538347633598246,3.478594554179222,1.7158747717031821,1.762854320485332,3.9741833292697395])                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |Hong Kong Disneyland is a great place to visit from Australia if you don't want to travel to America However having been to another Disneyland this one is going to need expanding quickly if it is going to cope with all the visitors The day I went it was a Monday afternoon and it was packed The waits weren't too long for the rides and I didn't need to use a fast pass The whole park could be seen and experienced in hours without missing anything The park itself was clean and easy to get around The map was easy to follow There was only one main parade that day and I was lucky enought to catch it It is the Water Works show and I got very wet Main street is the same as the other park with the same shops etc but if you are a die hard Disney fan this is not a problem The pirates had taken over AdventureLand Capt Jack Sparrow and his crew which made it a lot of fun As always plenty of merchandise to buy around the park however the mickey ears were only avaliable on main street in the largest shop in a back corner A great day out can't wait to see Disneyland Paris next                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
    |5             |4    |(1024,[10,35,43,44,59,72,88,93,100,102,109,115,123,125,131,146,159,167,174,181,195,197,202,215,264,295,301,308,333,340,342,344,346,347,358,363,366,386,409,410,413,423,441,443,495,496,498,502,550,557,558,559,560,567,587,590,591,608,612,613,643,656,660,679,683,688,729,731,741,802,808,818,820,822,825,832,843,844,855,873,876,879,890,912,925,929,942,957,984,995,996,1005,1006],[2.1914742108596585,3.0363276799050856,2.7343440668831,2.2424694475874043,3.0098264903617844,2.9606514626443667,2.518010655632218,2.6172833136705407,3.0084009733705632,3.8171967170582595,1.8242531870809897,5.586150351336325,2.366647057207249,3.6256780667833874,1.623056731086993,1.346787476821377,5.0203803915778655,3.1219196102404894,1.7088590225160791,3.072622988729262,2.308127186408648,1.7827280079722563,2.4472939630344857,1.6262700632181895,3.7884813366279375,5.527393196370747,1.3658056923958364,1.4451213638577318,2.550519428489459,6.7784859183542325,1.2878074037832108,4.044881480931667,1.5032689466716074,3.502432668735164,2.037773151111015,3.502432668735164,3.406768819607966,1.5429409052910974,2.5739211953967827,3.124049536498314,7.471223755590655,7.190666604933534,2.7221444179100036,4.232841393379888,2.226265896222711,1.2790855068534763,3.1353066910229486,2.906465118594101,1.9821449594518337,2.993928836132902,3.3497282816448557,2.999803171626358,3.7908932542663587,3.9824081523759243,4.164171675878621,1.9143893115209807,2.741590475403867,1.8166103333155523,2.531077023252643,2.5203396232060347,2.4483782678303396,3.7005989369631767,2.034719713624124,2.958390552795902,2.9360582267414106,5.400065809036331,5.701910938982502,1.4994814618704382,3.4648776835023813,3.6134294447071884,2.358432947550327,4.531229290930839,2.828139149539619,4.187642211800076,2.3133562590344265,2.226265896222711,2.6380497964630267,2.3691495610157505,4.277440031180478,4.587902131964079,2.980309238503548,1.6908960042374817,7.17126471302824,1.4010545152590537,3.0505890477516466,3.512633138118947,1.8680933774237913,0.7512782544532749,1.7158747717031821,1.762854320485332,2.4916102701360976,2.1560285055238593,5.96127499390461])                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |Having been to Disney World in Florida times and to Disneyland in California I couldn't pass up the opportunity to see Disney Hong Kong since we were going to be in China Disney Hong Kong is very different from the US parks in that it is open shorter hours is very small in comparison but like any Disney park it's alot of FUN True to Disney it's clean the people are friendly and helpful and it's a happy place We saw little if any queue jumping waiting for rides In fact the lines didn't seem bad Maybe because we were waiting for the English versions Prices for things are as you expect at Disney but t shirts and such are cheaper then Disney World Food was the biggest expense at the park considering the choices We probably would have been better off going with a Chinese meal We were able to do the rides we wanted to do multiple times without problem and then we sat for the parade It was HOT But the parade is a water parade and the staff makes sure you know you're going to get wet We had seats on the curb sitting on bags of purchases so as not to burn our back sides Then came the parade Yes it's a water parade but with all the Asians using umbrellas over us to protect themselves from the sum we didn't get wet It was a fun day If you go go early when they open It seemed to get more crowded as the day went on but we really sat in no lines longer then minutes and that was for a show                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
    |5             |4    |(1024,[16,31,44,83,88,93,108,109,116,120,131,132,146,161,185,197,217,218,231,242,243,245,248,259,260,261,273,322,323,343,346,358,387,394,406,407,424,432,433,441,444,454,455,458,488,489,496,498,510,549,550,579,588,598,603,618,625,630,637,653,657,684,688,690,699,704,715,729,741,812,816,840,847,857,863,868,869,933,941,957,963,987,988],[2.6592566879653483,2.208612046996743,2.2424694475874043,3.2050699785726597,2.518010655632218,2.6172833136705407,6.9693842680946805,0.9121265935404949,6.823463217900191,7.348760413316114,1.623056731086993,3.271382712386849,3.3669686920534425,4.742052811791337,5.561322129443347,1.7827280079722563,1.9193307418356567,5.451460513223063,3.881024758592232,1.897674952051541,2.8735646395797834,2.584120634765676,8.320087903212613,3.460521773119527,2.9009059851841044,2.181663047721467,3.1472374182247793,2.4899124768590037,6.096231271314256,5.630353331382022,3.0065378933432148,2.037773151111015,2.506446490912542,7.677498550887988,2.368398152088724,2.5937976261543785,3.7713553577100183,2.2197709610721628,4.965177000917296,5.444288835820007,2.9998902837026544,2.30204605364284,2.477270083443827,3.9359130238976414,1.549543041948325,3.014115274634002,1.2790855068534763,6.270613382045897,6.1025455146568675,2.3842975406426237,1.9821449594518337,1.7550954848564635,2.283935505248109,1.3391543687941583,3.7940133816026025,1.571080447556176,8.31931399104819,3.284438539738264,2.2591643261595746,1.8358276957194233,7.977827441206834,3.7120494437511717,1.8000219363454435,5.818942696086055,1.9503271381221863,1.6335680557696795,3.3503965075300615,4.072793527844644,1.7324388417511907,1.9472003356075678,1.458940930878359,2.6778477253535917,2.337338223720912,1.8676380385679312,1.2680347685391393,2.3724122066505666,3.414578585962023,3.4597757827828057,2.896650659613966,0.7512782544532749,3.8055378674545564,1.8576726835634396,1.5683768298228091])                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |I recently travelled to Disneyland with my year old daughter It was a long day as we arrived early before the park opened and were there until the end for the fireworksThe rides at Disney are surpassed by rides at Ocean Park but the level of comfort you experience at Disney is amazing Air Con and fans throughout the park meant that whenever we got a little bit hot we were able to take minutes in an Air Conditioned shop and cool off Probably the most frustrating thing I encountered were children pushing and cutting in lines When you are lining up and can see the front of the line the last thing you want to see is children streaming past you to a relative at the front of the line This happened a LOT especially while we were lining up to have photos taken with Disney characters who can only take so much in those suits before then need a minute break Nothing is more frustrating than having kids cut in front of you in line have their photos taken and then be told that Mickey has to take Overall Disney was an experince that I think you just HAVE to do that said Had I have been to any other Disney parks throughout the world I would probably have been disappointed with the HK park I will also write a review for Ocean Park which is another must do for families with children                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
    |5             |3    |(1024,[12,24,25,27,37,42,54,62,64,83,87,95,107,109,115,122,131,133,144,146,148,155,159,167,169,171,174,181,184,186,193,195,198,204,210,217,224,237,239,241,245,247,248,251,257,271,275,292,301,302,308,319,336,339,342,343,346,355,356,361,362,365,376,385,386,390,407,419,447,460,464,472,488,493,496,506,510,512,520,522,533,534,538,539,547,550,557,558,559,569,578,587,590,596,598,600,602,604,607,612,618,619,637,639,641,664,678,679,681,685,696,709,718,726,729,731,739,748,761,767,770,771,778,792,801,807,808,810,811,814,816,818,822,825,842,844,849,853,856,857,858,866,868,875,876,879,883,890,892,912,915,919,920,925,927,936,941,949,954,955,957,983,993,1012,1015],[3.658790711660863,3.3727022650443597,9.284706903136499,3.622163124675943,3.431832788271182,8.949476084327982,3.704401222912915,3.3976170485876476,4.322352587861301,6.4101399571453195,8.15253197113825,2.5607852191278764,2.655245987401941,0.9121265935404949,2.7930751756681627,1.5904432392761008,1.623056731086993,3.412442593693874,4.262351731529277,6.733937384106885,4.514617408395587,2.6582525037805462,2.5101901957889328,6.243839220480979,3.295133828855012,2.7900170693093416,10.253154135096475,3.072622988729262,6.077544142493724,3.6318591019148223,2.9017592275566884,2.308127186408648,3.421014074744015,3.3934212382363125,2.676824707361557,5.75799222550697,3.324009752356867,2.363901516982658,3.1272529562158518,1.8693086289784864,2.584120634765676,2.6004066195258444,2.7733626344042044,11.971411827928314,1.891439546034257,8.449019230355503,3.884439673692301,8.933082549919623,2.7316113847916728,3.470270631732277,4.3353640915731955,1.2349305576216125,2.6940131676596444,3.8585519027401736,1.2878074037832108,1.8767844437940073,1.5032689466716074,2.726433202182221,4.062437637449968,3.262160390644536,2.4197705155695246,2.617604495622876,3.3457282763115095,2.5856751197670262,1.5429409052910974,3.57170351103697,7.781392878463135,1.5957476797579722,3.2166980165677788,3.002247107796185,5.751282967049245,3.6204102758485286,1.549543041948325,3.4915966493938844,2.5581710137069527,2.888615090894871,12.205091029313735,3.9028521616367535,7.330982307571876,3.9651477490059923,5.253774894604366,5.309088532588418,2.9793863035049473,5.051474870879525,4.205748102724362,1.9821449594518337,1.496964418066451,6.699456563289711,1.499901585813179,3.9455632797298033,2.400712374457187,8.328343351757242,1.9143893115209807,2.792309771880317,4.017463106382475,2.6881358819430803,1.5117372940925327,2.3011092854244533,5.290308637936821,5.062154046505286,4.713241342668528,3.1197942109281755,2.2591643261595746,3.405355391810819,3.6949224789583712,2.0215358419239977,2.048988898203824,5.916781105591804,4.12715782813114,2.590126017359716,5.727310785034023,3.6828582509191916,3.269531431389126,3.2566675480272163,2.4436761167067864,1.4994814618704382,3.2272801258983157,2.536796062757854,1.8961124517336496,4.310060949349745,4.005506315696836,3.7015481539305086,3.1003455032959275,3.663249407430158,6.207992188103169,2.9453771315442756,2.358432947550327,3.002247107796185,5.0371847784293005,4.642413800090681,2.917881861756718,1.1328073227327098,2.093821105900038,4.626712518068853,2.3034528534059215,2.3691495610157505,1.9372272291574746,1.980784646370107,2.0178259180504563,1.8676380385679312,12.112244642677405,3.705354057668187,4.744824413301133,3.906342566576522,2.980309238503548,1.6908960042374817,2.706223331566576,2.3904215710094134,3.9075087474667045,4.203163545777161,4.689684826274021,16.038891806253023,5.2787403060231135,3.0505890477516466,2.332012624108955,3.260937148901792,2.896650659613966,3.534055857165676,3.0145929436184846,3.146692013248074,3.0051130178130996,3.478594554179222,3.395516942810531,3.499324663125297,4.538453856550098])|Our family just came back from Disneyland HK where we stayed at Disney's Hollywood hotel for nights Travelling with kids ages and and pieces of luggage we took a cab from the airport which was the easiest way to get to the resort We were however tempted to take the train because we wanted to see Mickey's resort line but in the end didn't luckily With big luggage and kids it would have been back breaking The Hollywood hotel was nicely decorated and colourful Although it was far less luxurious than the other Disneyland hotel it was quite suitable for kids There was plenty of choice at the buffet breakfast at the Chef Mickey restaurant Dinner at the same restaurant wasn't as nice and relatively expensive at HKD for the of us so we preferred to eat in the restaurants in the Park where we found the food quite tasty We particularly liked the Chinese food at Plaza Inn I wish I could tell you that we had a good time in the Park but to be honest after all our months of planning we failed realise that Friday th of October was a Chinese national holiday so the Park was packed clever us We got on a few rides like Buzz Light Year Winnie the Pooh and saw Philhar Magic but missed out on Autopia queue mins Golden MIckeys and Festival of the Lion King simply because the queues were too long At least I got to shake hands with Mickey which made me happy Fastpass is a good idea but is not available for all the rides basically you are reserving a place and have to come back at a pre determined time when you can enter via a shortened line up there is still a queue though After one and a half days at the Park on our third day we went to the city to do some shopping We took the bus to the Disney resort line which is right in front of the entrance to the Park connected to Sunny Bay station and got onto the Tung Chung line bound for Kowloon The Disney resort line is quite unique with Mickey window panes and handle bars as well as memorabilia inside the compartment By the way the free Disney bus service runs from the Park to Disneyland Hotel to Hollywood Hotel and then back to the Park so if you want to go from Hollywood Hotel to Disneyland Hotel you have to go to the Park first The trip in to town took about mins which is lengthy by HK standards Unlike the Airport Express the Tung Chung line makes several stops along the way We then came back just before dinner time and by then all of us were exhausted On the way back to the airport we took the cab which cost us around HKD They are cheaper than the limousines and there are plenty of them hiding around the corner of the hotel |
    |5             |2    |(1024,[26,43,75,83,109,169,179,194,195,290,329,342,365,395,402,430,441,445,565,583,590,608,654,660,690,729,731,742,857,903,957,971,1002],[2.487653227098256,2.7343440668831,3.8607765977622845,3.2050699785726597,0.9121265935404949,3.295133828855012,4.96385317955908,2.2101069751982676,1.154063593204324,2.841310363696892,2.294343833280748,1.2878074037832108,2.617604495622876,3.3530738853007778,4.257372468129669,2.802692483878455,2.7221444179100036,3.8140035264791043,1.2902747551859621,2.048443294843922,1.9143893115209807,1.8166103333155523,2.6010383314862078,2.034719713624124,2.9094713480430276,0.8145587055689288,1.4994814618704382,2.015888103650351,1.8676380385679312,3.3843902633457743,0.7512782544532749,3.4032389889044414,2.739411033930935])                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |I went to HK disneyland with my family and found most staff unwilling to greet nor smile The place was small and you were left without anything to do by pm I tried talking to the management in main street who just told me she was busy and to find someone else to talk to They were rude and too proud to understand what the real disney is all about I will not go there ever again                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
    |5             |2    |(1024,[109,131,146,210,257,261,262,285,305,322,376,405,467,472,488,495,496,502,526,532,579,598,618,619,656,737,742,749,750,822,863,890,957,962],[0.9121265935404949,1.623056731086993,0.6733937384106885,2.676824707361557,1.891439546034257,2.181663047721467,2.564738394414448,3.173772971850745,3.483927900154584,2.4899124768590037,3.3457282763115095,3.0456483262386693,4.072020188530964,3.6204102758485286,3.09908608389665,2.226265896222711,0.6395427534267382,2.906465118594101,2.996600033965867,3.268915098549893,1.7550954848564635,1.3391543687941583,1.571080447556176,3.1197942109281755,3.7005989369631767,3.8913046892119816,2.015888103650351,3.6380785799818423,3.566803731206254,2.093821105900038,1.2680347685391393,2.3904215710094134,0.7512782544532749,4.755584818009914])                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |Since long I read and heard a lot of Walt Disneys great attraction Disneyland But it was not being possible for me to view in own eyes visiting USA Last year it was a great opportunity for me to find the mini or replica of this great theme park built in Hong Kong It was a day long experience which was tremendous and the rides and live shows were extra ordinary                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
    |5             |4    |(1024,[26,52,238,241,245,273,300,348,457,469,561,579,685,822,890,924,927,957,1011],[2.487653227098256,4.260689220755663,3.9311225955750086,1.8693086289784864,2.584120634765676,3.1472374182247793,10.497493025428687,4.3805508271042894,3.1483291213899736,3.546990556498449,3.1277878586089214,1.7550954848564635,0.8633753391199054,2.093821105900038,2.3904215710094134,2.3794758081693166,2.332012624108955,1.5025565089065498,6.775708140129924])                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | DEC Experience all the fantasies that you have ever imagined in Disneyland Disneyland Hong Kong consist of Adventure land Fantasy land and Tomorrow Land However i spent most of the time in Fantasy Land I arrived there at am and only departed More                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
    |5             |5    |(1024,[31,47,195,209,217,259,299,308,311,333,342,346,357,467,493,496,502,511,513,557,561,590,608,612,631,633,678,685,704,729,816,822,835,845,858,883,890,946,968,986,1017],[2.208612046996743,1.9225317688967356,1.154063593204324,3.3253126841372826,1.9193307418356567,3.460521773119527,4.480930012411911,1.4451213638577318,2.9611042586674983,2.550519428489459,1.2878074037832108,1.5032689466716074,3.704401222912915,4.072020188530964,3.4915966493938844,0.6395427534267382,2.906465118594101,3.3692904568900235,2.6519159809947537,1.496964418066451,3.1277878586089214,1.9143893115209807,1.8166103333155523,2.531077023252643,4.442346546372755,2.3589288563132795,2.048988898203824,2.590126017359716,1.6335680557696795,0.8145587055689288,1.458940930878359,2.093821105900038,4.874638980518005,2.8530300371380757,3.0280611606693513,2.706223331566576,2.3904215710094134,4.442346546372755,2.5629119098884137,1.4159150542983712,2.602935865262399])                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |What a magical time The whole place is perfectly run with friendly helpful staff and virtually no cues The most we cued up for was to see Micky and Mini mouse at the begining for mins a fraction of time compared to any ride in the US Although there were not as many action packed rides we still did not have time to go round the whole venue Well worth a trip close to Hong Kong island and also very cheap for Disney In all a must if your out there                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
    |5             |5    |(1024,[62,95,103,109,116,131,133,146,180,196,198,204,226,241,242,253,256,261,271,273,298,308,325,345,373,397,411,426,431,434,437,442,462,467,474,488,508,521,527,535,565,583,595,597,600,612,631,636,660,664,673,679,694,704,718,731,761,790,801,829,835,845,857,879,892,957,977,987,995,1005,1011,1015,1020],[3.3976170485876476,2.5607852191278764,2.362905004692919,0.9121265935404949,3.4117316089500953,1.623056731086993,3.412442593693874,1.346787476821377,2.934734015556955,2.2018064060968636,3.421014074744015,3.3934212382363125,3.400424067974632,3.7386172579569728,1.897674952051541,1.7135307154143558,2.9909846705909073,2.181663047721467,2.112254807588876,3.1472374182247793,3.594479695927526,1.4451213638577318,2.1808324828896675,4.213640803523271,3.0436788524361376,3.702498272766829,8.157843054343315,2.953884010694011,4.140325538505788,3.556802573076957,4.296195909212573,3.5260558144986,3.5961876382726823,4.072020188530964,5.732247027770191,1.549543041948325,2.514817156039079,3.8798890401283046,3.141796708394661,1.949009397855735,1.2902747551859621,4.096886589687844,2.9391548892469483,2.645949771716183,2.6881358819430803,1.2655385116263216,4.442346546372755,3.2044921108882747,2.034719713624124,2.0215358419239977,2.6159996164467625,2.958390552795902,2.696095778617171,1.6335680557696795,3.269531431389126,2.9989629237408764,1.8961124517336496,4.510343897618204,3.1039960940515847,3.268299145342861,2.4373194902590023,14.265150185690379,1.8676380385679312,1.6908960042374817,3.9075087474667045,0.7512782544532749,2.9937884107453363,1.8576726835634396,3.525708640970664,2.1560285055238593,3.387854070064962,4.538453856550098,2.1775171048010558])                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |Our family went to Disneyland last March There were twelve of us When we arrived we immediately checked the show schedules and organized our day We walked through the Main Street U S A on our way to the Adventureland Where we experienced the Jungle River Cruise Cruising through the River filled with adventure and surprises After which we watched the Festival of the Lion King We then had lunch at the nearby Tahitian Terrace Restaurant We then proceeded to the Fantasyland where we watched the Golden Mickeys Rode the Park Train Watched the Adventures of Winnie the Pooh Strolled Through the Sleeping Beauty Castle and The Fantasy Gardens where we had our pictures with the characters taken Watched d at the Mickey's Philharmagic We also went to the Tomorrowland Where we road the Space Mountain Autopia Buzz Light Year and saw the Interactive Stitch Encounter Show Before the park closed we watched the awesome display of fireworks                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
    |5             |5    |(1024,[36,109,122,146,195,245,247,295,325,329,346,361,390,432,448,478,516,550,598,615,629,637,649,680,683,685,704,792,849,858,865,868,871,903,919,924,927,968,984,1005],[6.221003601244677,3.6485063741619794,3.1808864785522015,0.6733937384106885,1.154063593204324,2.584120634765676,2.6004066195258444,1.842464398790249,2.1808324828896675,4.588687666561496,3.0065378933432148,3.262160390644536,3.57170351103697,2.2197709610721628,3.45531148965412,5.669007061216941,4.043543688090007,3.9642899189036673,2.6783087375883166,2.5499188278708047,4.068722195754099,2.2591643261595746,2.030603376715591,4.20292112491599,2.9360582267414106,0.8633753391199054,1.6335680557696795,1.2210831358100527,1.9372272291574746,3.0280611606693513,2.4899124768590037,2.3724122066505666,2.4689297515276083,3.3843902633457743,2.004861475781628,2.3794758081693166,4.66402524821791,2.5629119098884137,1.7158747717031821,2.1560285055238593])                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |We visited in the end of March and stayed at the Hollywood Hotel We bought the two day pass with the intention of spending a half day the st day Our kids are & and we could have spent the two full days as they had to see EVERYTHING and go on EVERYTHING It was also fairly busy so some queues were listed as up to minutes however turned out to be On busy days in school holidays be patient and soak in the detail I can understand that adults can do this park in just a day however if you are travelling with kids make sure that you give yourself the time to see everything                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
    |5             |5    |(1024,[7,40,87,109,122,185,216,241,258,271,300,301,319,325,330,337,342,357,358,447,474,481,496,514,519,559,662,704,707,729,748,750,766,816,868,883,924,929,942,957,988,1013],[2.854657376797451,3.586829849867122,2.0381329927845626,0.9121265935404949,1.5904432392761008,1.8537740431477823,2.6118389447380785,1.8693086289784864,3.0595443314807507,2.112254807588876,2.624373256357172,2.7316113847916728,1.2349305576216125,2.1808324828896675,4.983241326905545,2.4717021403285826,1.2878074037832108,3.704401222912915,2.037773151111015,3.2166980165677788,2.8661235138850953,3.7971432746115297,0.6395427534267382,3.337116099862167,4.289334818832628,4.499704757439537,4.021070832237948,1.6335680557696795,4.226400147277031,1.6291174111378577,2.536796062757854,2.3778691541375028,2.6400189470829076,2.917881861756718,2.3724122066505666,2.706223331566576,2.3794758081693166,1.7563165690594735,1.8680933774237913,0.7512782544532749,3.1367536596456183,4.165681110127476])                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |We spent one and a half days at HKDL and although it is much smaller that LA Disney or Euro Disney it is still lots of fun and a great way to spend the day The que times were very short and while we were they it wasn't as crowded as I thought it would be The rides were more suitable for younger children more so than teens but there is still lots of fun to be had for the older ones Lots of food oulets in each adventure land serving mainly Asian food but also a little western but not much for vegetarians In all a great place to have fun                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
    |5             |5    |(1024,[9,37,43,72,97,122,146,200,215,216,232,239,242,261,293,308,322,342,346,369,409,462,484,496,568,594,598,602,612,637,638,648,652,729,750,822,845,890,893,919,924,941,942,957,988,996],[2.2650175621412862,3.431832788271182,2.7343440668831,2.9606514626443667,2.669354383996392,1.5904432392761008,1.346787476821377,2.232147625410842,1.6262700632181895,2.6118389447380785,3.0436788524361376,3.1272529562158518,1.897674952051541,2.181663047721467,3.000832346890556,1.4451213638577318,2.4899124768590037,1.2878074037832108,1.5032689466716074,1.6757518480731843,2.5739211953967827,3.5961876382726823,3.240966742402649,0.6395427534267382,3.554340528337182,3.8044846814698974,1.3391543687941583,1.5117372940925327,1.2655385116263216,2.2591643261595746,3.943141971040793,2.705521577151812,3.2463737847138954,1.6291174111378577,1.1889345770687514,6.281463317700114,2.8530300371380757,9.561686284037654,3.5592706944450407,2.004861475781628,2.3794758081693166,2.896650659613966,1.8680933774237913,3.0051130178130996,1.5683768298228091,2.4916102701360976])                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |We have read many unfavourable reviews of the Hong Kong Disneyland To be fair we decided to see the place for ourselves Hong Kong Disneyland is made for families and for people who wish to rekindle the magical experiences of the Disney characters With our kids we enjoyed ourselves very much every minute of our days spent at the park Never a dull moment The most praiseworthy aspect is the localization of Disneyland for Asians We were pleased that there were languages used everywhere at the Park the Putonghua Cantonese and English Great job We will certainly visit the Hong Kong Disneyland again A Disney Fan                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
    |5             |5    |(1024,[36,40,44,104,109,123,131,146,157,185,197,202,214,257,276,333,346,385,602,604,608,644,649,660,680,689,729,750,792,811,822,832,849,868,890,919,924,941,957,987],[2.073667867081559,3.586829849867122,2.2424694475874043,3.183908193744788,1.8242531870809897,2.366647057207249,1.623056731086993,2.0201812152320655,4.007983064546176,1.8537740431477823,1.7827280079722563,2.4472939630344857,2.2906306705090067,3.782879092068514,3.50555036416775,2.550519428489459,1.5032689466716074,5.1713502395340525,1.5117372940925327,2.3011092854244533,1.8166103333155523,2.054278468220644,4.061206753431182,2.034719713624124,2.101460562457995,2.5937976261543785,0.8145587055689288,1.1889345770687514,1.2210831358100527,2.5185923892146502,4.187642211800076,2.226265896222711,1.9372272291574746,2.3724122066505666,2.3904215710094134,2.004861475781628,2.3794758081693166,2.896650659613966,1.5025565089065498,3.7153453671268792])                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |Myself and my husband and our four year old son visited Disneyland in early November We stayed in the Disneyland Hotel and spent a day and a half in the park We loved it We have visited almost all the other Disney parks bar Tokyo next on the list and have to say everything about the Hong Kong park was great The park was spotless clean the staff were very friendly no queues and plenty of things to do and see Our son loved it would definitely recommend a day or two visit for families with young children                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
    |5             |4    |(1024,[8,25,36,62,68,72,73,76,80,91,101,104,109,110,114,115,117,122,134,140,157,158,174,178,185,186,187,196,202,215,216,225,231,241,259,286,294,295,301,308,311,316,319,323,331,333,336,342,346,355,358,364,367,369,376,386,390,391,407,413,449,454,478,488,493,496,497,510,518,520,527,529,535,543,549,557,559,560,565,566,578,587,590,598,602,604,608,610,612,615,630,653,660,675,679,704,715,726,729,732,738,741,749,752,758,771,778,785,794,803,811,816,828,832,846,853,856,863,868,873,879,886,893,903,912,927,932,937,951,957,959,971,983,986,990,1005,1009,1016],[3.7908932542663587,4.642353451568249,2.073667867081559,3.3976170485876476,8.558261297316772,5.9213029252887335,2.5947390980582203,3.4680124448486844,4.608857497140067,3.669769778943275,3.623040702617302,3.183908193744788,0.9121265935404949,3.131540208227472,3.6958663204630775,2.7930751756681627,11.017600452194678,1.5904432392761008,2.744139175126881,2.6727430690419087,2.003991532273088,3.6125602573426017,1.7088590225160791,2.5229662116123452,5.561322129443347,3.6318591019148223,4.943918964658263,2.2018064060968636,2.4472939630344857,8.131350316090947,2.6118389447380785,4.611133541621701,3.881024758592232,1.8693086289784864,3.460521773119527,2.906036394043054,3.8878662904089487,1.842464398790249,1.3658056923958364,1.4451213638577318,2.9611042586674983,4.08447709701626,1.2349305576216125,3.048115635657128,2.82615855867629,2.550519428489459,2.6940131676596444,6.4390370189160535,4.509806840014822,2.726433202182221,2.037773151111015,5.672920362617193,2.9821576678653305,3.3515036961463687,3.3457282763115095,1.5429409052910974,3.57170351103697,4.2979185625240195,2.5937976261543785,4.482734253354392,4.229615584131006,2.30204605364284,2.8345035306084707,1.549543041948325,3.4915966493938844,1.9186282602802145,2.828139149539619,2.034181838218956,3.3104304942304856,1.832745576892969,3.141796708394661,12.47446986044658,1.949009397855735,2.7547678256696697,2.3842975406426237,2.993928836132902,1.499901585813179,3.7908932542663587,1.2902747551859621,3.0055560212383323,2.400712374457187,4.164171675878621,1.9143893115209807,1.3391543687941583,1.5117372940925327,2.3011092854244533,3.6332206666311047,3.6968110536462393,2.531077023252643,5.099837655741609,3.284438539738264,1.8358276957194233,2.034719713624124,3.6073609388708077,2.958390552795902,3.267136111539359,3.3503965075300615,3.2566675480272163,1.6291174111378577,3.9323180529797823,2.3848064465060244,5.197316525253572,7.2761571599636845,3.31364696532132,3.7703375459173176,3.7015481539305086,3.1003455032959275,8.669594385818769,2.953434471464144,5.478096510081374,2.5185923892146502,1.458940930878359,3.0382827147408893,2.226265896222711,3.2184539432700436,1.980784646370107,2.0178259180504563,2.5360695370782786,2.3724122066505666,4.587902131964079,1.6908960042374817,3.27676540313044,3.5592706944450407,3.3843902633457743,5.604218061036215,2.332012624108955,2.5610887553268986,2.8865111557615264,3.172653150905056,2.2538347633598246,3.836572996021398,3.4032389889044414,10.435783662537666,2.8318301085967423,8.011012631393672,2.1560285055238593,5.749620456562824,3.9110154722759143])                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |The place itself is magical Most of the staff members are friendly and accommodating We were there on a Monday and the place was packed The lines were a bit too long and a lot of people were rude and uncouth There were a couple of Chinese people who were trying to cut lines and a lot of them can't understand english They should place staff members who can guide guard people waiting in line I also noticed that some would deliberately disobey the height requirement for the rides I even saw a toddler in the Space Mountain ride During the parade the Disney employees who were in charge of the rope to keep the crowd away from the floats were cranky and ill mannered One even snapped at my kid for merely touching the rope Sure they do it every single day and it's quite tiring doing the same task over and over again but hey they're working for Disney and the job requirements are courtesy and a smiling face Also for tourists especially the OLDER TOURISTS they should realize that Disneyland is really made for children and they should avoid pushing children aside and blocking little people half their size just so they could see Mickey Mouse closer It was downright disgusting seeing this middle aged Caucasian man blocking the view of the parade with his huge luggage and practically having a tantrum when I asked him to put it aside so my kids could at least see the parade What an embarrassment to his generation Well it's not really Disneyland's responsibility how we were all brought up But they really should add more people to guide guard tourists and train them well to be more accommodating more courteous and speak better English Aside from those Disneyland is really a beautiful magical place The place is wonderful itself It was like stepping into another dimension where everything is pure fun and happy There were restaurants and food carts at every corner The restrooms were all big no lines and clean The rides and attractions were enjoyable enough that you would want to see ride them again There were just so many wonderful things to do that a single visit isn't enough I loved Disneyland and we will definitely come back here again                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
    |5             |5    |(1024,[36,78,101,109,122,145,174,185,195,242,253,300,301,333,343,432,484,500,517,565,567,579,602,612,623,644,678,682,721,729,731,761,822,857,890,921,931,936,957],[2.073667867081559,3.0363276799050856,3.623040702617302,0.9121265935404949,1.5904432392761008,4.255718208033643,1.7088590225160791,5.561322129443347,1.154063593204324,1.897674952051541,1.7135307154143558,2.624373256357172,1.3658056923958364,2.550519428489459,1.8767844437940073,2.2197709610721628,3.240966742402649,4.013258292501154,1.9984417434881927,1.2902747551859621,1.9912040761879621,1.7550954848564635,1.5117372940925327,1.2655385116263216,4.188600371563584,2.054278468220644,2.048988898203824,2.807730277908412,2.5887913578762345,0.8145587055689288,1.4994814618704382,1.8961124517336496,2.093821105900038,1.8676380385679312,2.3904215710094134,3.0422042883940708,2.1841588892832857,3.260937148901792,0.7512782544532749])                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |It has always been a dream of mine to visit disneyland so we booked a trip to Hong Kong with my family my children and my sisters and their children It was awesome we went on seperate days once at night to experience the magical fireworks which I definetly recommend and during the day My children and nephews had a ball as did all the adults the food was excellent as was the price for everything at disney land The characters are all there as soon as the gates open we can't wait to go back                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
    +--------------+-----+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    only showing top 20 rows
    


#### Evaluate Model


```python
# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")

accuracy = evaluator.evaluate(predictions)
print(accuracy)
#print("Test Error = %g" % (1.0 - accuracy))
```

    0.5473696461824954
    Test Error = 0.45263


#### Model Summary


```python
rfModel = model.stages[2]
print(rfModel)  # summary only
```

    RandomForestClassificationModel: uid=RandomForestClassifier_b17042230059, numTrees=50, numClasses=5, numFeatures=1024


#### Multi-label Logistic Regression


```python
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Index labels, adding metadata to the label column.
# Fit on whole dataset to include all labels in index.
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(tf_idf4)

# Automatically identify categorical features, and index them.

featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=5).fit(tf_idf4)

# Split the data into training and test sets (20% held out for testing)
(trainingData, testData) = tf_idf4.randomSplit([0.8, 0.2])

# Train a RandomForest model.
logr = LogisticRegression(labelCol="indexedLabel", featuresCol="indexedFeatures")

# Convert indexed labels back to original labels.
labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                               labels=labelIndexer.labels)

# Chain indexers and forest in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, logr, labelConverter])

# Train model.  This also runs the indexers.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")

accuracy = evaluator.evaluate(predictions)
print(accuracy)
```

    0.5898063914159085


#### Using Logistic Regression to make predictions on external reviews. Use case: Reviews from Trip Advisor on Disneyland


```python
data = [("Everyone knows how amazing Disney is! It is an all day affair and more! The different lands are creative and fun with rides around each theme. It is the cleanest amusement park even when there are a lot of people!",5),("We can’t wait to go back. I was lucky enough to take my family just before covid hit North America hard and shut the parks down. It is truly the most magical place on earth. Dreams came true and the magic was everywhere.", 5), ("Disneyland has changed dramatically. I remember happy quaint park. Now too crowded Spent more time walking and waiting hours for rides.Got on a few rides even with fast passes in four days.We got very little accomplished in four days. Miss the days of characters roaming. Waiting in line for them is hard for young children", 1)]

rdd = spark.sparkContext.parallelize(data)

dfFromRDD1 = rdd.toDF()

columns = ["review","label"]
dfFromRDD1 = rdd.toDF(columns)
dfFromRDD1.printSchema()
```

    root
     |-- review: string (nullable = true)
     |-- label: long (nullable = true)
    



```python
# Break text into tokens at non-word characters
tokenizer10 = Tokenizer(inputCol='review', outputCol='words').transform(dfFromRDD1)
```


```python
tokenizer10.show()
```

    +--------------------+-----+--------------------+
    |              review|label|               words|
    +--------------------+-----+--------------------+
    |Everyone knows ho...|    5|[everyone, knows,...|
    |We can’t wait to ...|    5|[we, can’t, wait,...|
    |Disneyland has ch...|    1|[disneyland, has,...|
    +--------------------+-----+--------------------+
    



```python
# Remove stop words
remover10 = StopWordsRemover(inputCol='words', outputCol='terms')\
      .transform(tokenizer10)
remover10.show()
```

    +--------------------+-----+--------------------+--------------------+
    |              review|label|               words|               terms|
    +--------------------+-----+--------------------+--------------------+
    |Everyone knows ho...|    5|[everyone, knows,...|[everyone, knows,...|
    |We can’t wait to ...|    5|[we, can’t, wait,...|[can’t, wait, go,...|
    |Disneyland has ch...|    1|[disneyland, has,...|[disneyland, chan...|
    +--------------------+-----+--------------------+--------------------+
    



```python
# Apply the hashing trick and transform to TF-IDF
hasher10 = HashingTF(inputCol='terms', outputCol="hash", numFeatures=1024)\
      .transform(remover10)
hasher10.show()
```

    +--------------------+-----+--------------------+--------------------+--------------------+
    |              review|label|               words|               terms|                hash|
    +--------------------+-----+--------------------+--------------------+--------------------+
    |Everyone knows ho...|    5|[everyone, knows,...|[everyone, knows,...|(1024,[109,110,14...|
    |We can’t wait to ...|    5|[we, can’t, wait,...|[can’t, wait, go,...|(1024,[18,19,131,...|
    |Disneyland has ch...|    1|[disneyland, has,...|[disneyland, chan...|(1024,[10,103,115...|
    +--------------------+-----+--------------------+--------------------+--------------------+
    



```python
 #Convert hashed symbols to TF-IDF
tf_idf_10 = IDF(inputCol = 'hash', outputCol = 'features')\
      .fit(hasher10).transform(hasher10)
      
tf_idf_10.select('features', 'label','review','words','terms','hash').show(3, truncate=True)
```

    +--------------------+-----+--------------------+--------------------+--------------------+--------------------+
    |            features|label|              review|               words|               terms|                hash|
    +--------------------+-----+--------------------+--------------------+--------------------+--------------------+
    |(1024,[109,110,14...|    5|Everyone knows ho...|[everyone, knows,...|[everyone, knows,...|(1024,[109,110,14...|
    |(1024,[18,19,131,...|    5|We can’t wait to ...|[we, can’t, wait,...|[can’t, wait, go,...|(1024,[18,19,131,...|
    |(1024,[10,103,115...|    1|Disneyland has ch...|[disneyland, has,...|[disneyland, chan...|(1024,[10,103,115...|
    +--------------------+-----+--------------------+--------------------+--------------------+--------------------+
    



```python
trainingData.show(3)
```

    +---------+------+--------------------+-----+--------------------+--------------------+--------------------+--------------------+
    |Review_ID|Rating|         Review_Text|label|               words|               terms|                hash|            features|
    +---------+------+--------------------+-----+--------------------+--------------------+--------------------+--------------------+
    |  3530053|     2|Obviously I haven...|    2|[obviously, i, ha...|[obviously, visit...|(1024,[7,38,40,59...|(1024,[7,38,40,59...|
    |  3924467|     4|Visited Hkg Disne...|    4|[visited, hkg, di...|[visited, hkg, di...|(1024,[24,30,47,6...|(1024,[24,30,47,6...|
    |  4020946|     2|The park is small...|    2|[the, park, is, s...|[park, small, tin...|(1024,[1,7,10,18,...|(1024,[1,7,10,18,...|
    +---------+------+--------------------+-----+--------------------+--------------------+--------------------+--------------------+
    only showing top 3 rows
    



```python
tf_idf_10.show(3)
```

    +--------------------+-----+--------------------+--------------------+--------------------+--------------------+---------+
    |              review|label|               words|               terms|                hash|            features|Review_ID|
    +--------------------+-----+--------------------+--------------------+--------------------+--------------------+---------+
    |Everyone knows ho...|    5|[everyone, knows,...|[everyone, knows,...|(1024,[109,110,14...|(1024,[109,110,14...|     1254|
    |We can’t wait to ...|    5|[we, can’t, wait,...|[can’t, wait, go,...|(1024,[18,19,131,...|(1024,[18,19,131,...|     1254|
    |Disneyland has ch...|    1|[disneyland, has,...|[disneyland, chan...|(1024,[10,103,115...|(1024,[10,103,115...|     1367|
    +--------------------+-----+--------------------+--------------------+--------------------+--------------------+---------+
    



```python
tf_idf_10 = tf_idf_10.withColumn(
    'Review_ID',
    F.when((F.col("label") == 1), 1367)\
    .when((F.col("label") == 5) , 1254)\
    .otherwise(0)
)
tf_idf_10.show(3)
```

    +--------------------+-----+--------------------+--------------------+--------------------+--------------------+---------+
    |              review|label|               words|               terms|                hash|            features|Review_ID|
    +--------------------+-----+--------------------+--------------------+--------------------+--------------------+---------+
    |Everyone knows ho...|    5|[everyone, knows,...|[everyone, knows,...|(1024,[109,110,14...|(1024,[109,110,14...|     1254|
    |We can’t wait to ...|    5|[we, can’t, wait,...|[can’t, wait, go,...|(1024,[18,19,131,...|(1024,[18,19,131,...|     1254|
    |Disneyland has ch...|    1|[disneyland, has,...|[disneyland, chan...|(1024,[10,103,115...|(1024,[10,103,115...|     1367|
    +--------------------+-----+--------------------+--------------------+--------------------+--------------------+---------+
    


#### Multi-label Decision Trees


```python
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Index labels, adding metadata to the label column.
# Fit on whole dataset to include all labels in index.
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(tf_idf4)

# Automatically identify categorical features, and index them.

featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=5).fit(tf_idf4)

# Split the data into training and test sets (20% held out for testing)
(trainingData, testData) = tf_idf4.randomSplit([0.8, 0.2])

# Train a RandomForest model.
tre = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")

# Convert indexed labels back to original labels.
labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                               labels=labelIndexer.labels)

# Chain indexers and forest in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, tre, labelConverter])

# Train model.  This also runs the indexers.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")

accuracy = evaluator.evaluate(predictions)
print(accuracy)
```

    0.5438762253454589


Thank you!
