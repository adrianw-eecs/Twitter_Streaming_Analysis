"""
    This Spark app connects to a script running on another (Docker) machine
    on port 9009 that provides a stream of raw tweets text. That stream is
    meant to be read and processed here, where top trending hashtags are
    identified. Both apps are designed to be run in Docker containers.

    To execute this in a Docker container, do:
    
        docker run -it -v $PWD:/app --link twitter:twitter eecsyorku/eecs4415

    and inside the docker:

        spark-submit spark_app.py

    For more instructions on how to run, refer to final tutorial 8 slides.

    Made for: EECS 4415 - Big Data Systems (York University EECS dept.)
    Modified by: Tilemachos Pechlivanoglou
    Based on: https://www.toptal.com/apache/apache-spark-streaming-twitter
    Original author: Hanee' Medhat

"""

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import re
import sys
import csv
import requests


# Words used for sentiment analysis
positive_words = open("./positive.txt").readlines()
negative_words = open("./negative.txt").readlines()
# list of tags for each topic
cat1_tags = ['#country', '#russia', '#usa', '#germany', '#uk', '#france', '#canada', '#australia', '#eu']
cat2_tags = ['#tsla', '#appl', '#goog', '#uber', '#twtr', '#sbux', '#adbe', '#amzn', '#bidu', '#fb']
cat3_tags = ['#honda', '#toyota', '#ford', '#gmc', '#lincon', '#bmw', '#jeep', '#mini', '#nissan', '#ram']
cat4_tags = ['#sunny', '#cloudy', '#windy', '#rainy', '#hailing', '#snowing', '#cold', '#hot', '#raining', '#thunder']
cat5_tags = ['#processor', '#cpu', '#gpu', '#hdd', '#sdd', '#mouse', '#keyboard', '#monitor', '#pc', '#motherboard']

# Full list of tags used to filter tweets
full_tag_list = cat1_tags + cat2_tags + cat3_tags + cat4_tags + cat5_tags

# Topic words for each category
cat1_word = 'Countries'
cat2_word = 'Stocks'
cat3_word = 'Car Brands'
cat4_word = 'Weather'
cat5_word = 'Computer Parts'

# Create file to save data to
output_file = open("output.csv", "a+")
writer = csv.writer(output_file)
# Write a line that has all the topic categories so the graphing file knows which topic is which
writer.writerow(["Time", cat1_word, cat2_word, cat3_word, cat4_word, cat5_word])


# Used to filter tweets. Checks if input text has any of the hashtags in the categories above
# Returns true if tweet contains any of the hashtags, otherwise false
def check_topic(text):
    text = clean_input(text)
    for word in text.split(" "):
        if word.lower() in full_tag_list:
            return True
    return False


# Used as intermediate method to find topic of the tweet, first cleans the input data and then calls
# the determine topic which returns the cat_word associated with the topic
def process_topic(text):
    text = clean_input(text)
    tag = determine_topic(text)
    return tag

# Used as intermediate method to find the sentimental score assiociated with the tweet, first cleans the input
# data and then calls the sentiment_analysis which returns the score for each tweet passed
def process_sentiment(text):
    text = clean_input(text)
    senti = sentiment_analysis(text, positive_words, negative_words)
    return senti

# determines topic by checking if the tweet is in any of the category tags
# then return the category word
def determine_topic(text):
    for word in text.split(" "):
        if word.lower() in cat1_tags:
            return cat1_word
        if word.lower() in cat2_tags:
            return cat2_word
        if word.lower() in cat3_tags:
            return cat3_word
        if word.lower() in cat4_tags:
            return cat4_word
        if word.lower() in cat5_tags:
            return cat5_word

# clean input takes the text and removes punctuation, spacing and symbols that are not hashtags
def clean_input(input):
    # this function will clean the input by removing unnecessary punctuation

    # input must be of type string
    if type(input) != str:
        raise TypeError("input not a string type")
    # next, the input must be stripped of punctuation excluding hashtags since we need them
    temp = re.sub(r'[^\w#]', ' ', input)

    # now, any contractions are collapsed by removing the apostrophes
    temp = re.sub(r'[\']', '', input)
    # any numbers will also be removed since they hold very little meaning later on
    temp = re.sub(r'[\d]', '', temp)

    # replace all whitespace with the space character, this joins all the text into one scenetence
    temp = re.sub(r'[\s]', ' ', temp)
    return temp

# the general idea behind the sentiment method is to is to check if every word in the tweet is positive
# negative or neutral, if positive increase positive score, if negative increase neagtive score else do nothing
# Once values are calculate, if positive score is greter than neagtive score * threshhold then positive
# if negative score is greater than positive score * threshhold then negative
def sentiment_analysis(text, positive_words, negative_words):
    p_score = 0
    n_score = 0
    cleaned_text = clean_input(text)
    for word in cleaned_text.split(" "):
        for wordp in positive_words:
            wordp = wordp.replace("\n","")
            if word == wordp:
                p_score = p_score + 1
        for wordn in negative_words:
            wordn = wordn.replace("\n", "")
            if word == wordn:
                n_score = n_score + 1
    if p_score > 1.25 * n_score:
        return 1
    elif n_score > 1.25 * p_score:
        return -1
    else:
        return 0

# Spark configuration given to us by you, thank you
# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from spark context, interval size 2 seconds
ssc = StreamingContext(sc, 2)
# setting a checkpoint for RDD recovery (necessary for updateStateByKey)
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("twitter",9009)
# gets one whole tweet
tweets = dataStream
# filter the words to tweets with the hashtags we are looking for
filtered_tweets = tweets.filter(check_topic)

# map each hashtag to be a pair of (Topic, sentiment value, 1)
#  with this we can use the count to take the average, and keep track of the sentiment value for each topic
tweet_sentiScore_counts = filtered_tweets.map(lambda x: (process_topic(x), (process_sentiment(x), 1)))


# adding the count of each topic to its last count and summing the sentimental score
def aggregate_tags_count(new_values, total_sum):
    # since the count is the second value in the array new_values, total_sum
    # take sum of the count
    count = sum(x[1] for x in new_values) + (total_sum[1] if total_sum else 0)
    # since the sentiment value is the first value in the array new_values, total_sum
    # take sum of the the sentiment values
    sent = sum(x[0] for x in new_values) + (total_sum[0] if total_sum else 0)
    return sent, count


# do the aggregation, note that now this is a sequence of RDDs
tweet_totals = tweet_sentiScore_counts.updateStateByKey(aggregate_tags_count)


# process a single time interval
def process_interval(time, rdd):
    # print a separator
    print("----------- %s -----------" % str(time))
    try:
        # For each process interval create a dictionary and send values from dictionary
        # to the file
        dictionary = {}
        dictionary[cat1_word] = ''
        dictionary[cat2_word] = ''
        dictionary[cat3_word] = ''
        dictionary[cat4_word] = ''
        dictionary[cat5_word] = ''
        for x in rdd.collect():
            dictionary[x[0]] = x[1][0] / x[1][1]
        print("{},{},{},{},{},{}".format(str(time).split(" ")[1], dictionary[cat1_word],
                                         dictionary[cat2_word], dictionary[cat3_word],
                                         dictionary[cat4_word], dictionary[cat5_word]),
              file=output_file)

    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


# do this for every single interval
tweet_totals.foreachRDD(process_interval)


# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()



