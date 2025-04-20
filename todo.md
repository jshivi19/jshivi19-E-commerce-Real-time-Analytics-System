# TODO List

## Kafka Setup
- [ ] Install Kafka and Zookeeper
- [ ] Create Kafka topics (`tweets_topic`, `user_data_topic`)

## Kafka Producers
- [ ] Modify producer script to handle tweet-specific data

## Kafka Consumers
- [x] Write a consumer script to read data from Kafka topics

## Twitter API Integration
- [ ] Add Tweepy to `requirements.txt`
- [x] Implement `TwitterStreamListener` to fetch real-time tweets
- [x] Publish tweets to Kafka topics

## Spark Streaming
- [x] Set up Apache Spark for streaming
- [x] Consume Kafka data using Spark Streaming
- [x] Perform basic analysis (e.g., counting hashtags)

## Storing Processed Data in PostgreSQL
- [x] Install PostgreSQL and set up the database (`twitter_data`)
- [x] Create tables for tweets and processed data
- [x] Write a script to store processed data into the database

## Batch Mode Execution
- [x] Use SQL or Spark SQL to process the stored dataset in batch mode
- [x] Compare performance of streaming and batch processing

## Evaluation & Performance Comparison
- [x] Measure execution time and resource usage
- [x] Ensure accuracy of results from both modes
