# Spark-Twitter-Sentiment-Analysis

Sentiment Analysis of a Twitter Topic with Spark Structured Streaming

This project is about Sentiment Analysis of a desired Twitter topic with Apache Spark Structured Streaming, Python modules Tweepy(for streaming twitter) and Afinn(for scores: 0 for neutral , values<0 for negative and values>0 from positive). You can learn sentiment status of a topic that is desired.

Sentiment analysis is used for the interpretation and classification of emotions (positive, negative and neutral) within text data using text analysis techniques. Sentiment analysis allows businesses to identify customer sentiment toward products, brands or services in online conversations and feedback


For example, company can use sentiment analysis to automatically analyze NEGATIVE, NEUTRAL or POSITIVE reviews about their product, and discovered that customers were happy about their pricing but complained a lot about their customer service. Thus sentiment analysis can help to improve the business.

I have developed most common program sentiment analysis using spark streaming analytics on twitter. Please follow below notes:

1) To create twitter API and keys follow this video https://www.youtube.com/watch?v=KPHC2ygBak4

2) Now install tweepy package using command : pip3 install tweepy

Note: tweepy is module for python which streams tweets from twitter. For more details refer documentation http://docs.tweepy.org/en/latest/

3) Install afinn package using command : pip3 install afinn

Note: AFINN sentiment analysis in Python: Wordlist-based approach for sentiment analysis. For more details refer documentation https://pypi.org/project/afinn/

4) Note By default spark runs on python 2.7 version make it to run in python3+ version else above installed packages won't work as we installed using pip3 (which is for python3+). Alternately you can install above packages using pip(for python2+) if you wish to run on python2+ version in spark.


5) first run test_twitter.py file to get a glimpse of twitter streaming.

6) second run stream_tweets.py file for atleast 10 mins to store good amount of tweets for analysis.

7) after 10 mins forcily stop streaming by pressing ctrl+c or ctrl+d whichever works.

8) Now run sparkStreaming.py file for sturctured streaming analytics to do sentiment analysis on the twitter data.


