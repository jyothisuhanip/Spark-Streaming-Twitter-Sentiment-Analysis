
Version Compatibility:

Python : 3.7
Spark : 3.3.0
JDK: 11.0.15.1

Steps to run the code:

1. We have to start the Kafka Environment
1.1 Go inside the kafka folder
1.2 Zookeeper: bin/zookeeper-server-start.sh config/zookeeper.properties
1.3 Kafka: bin/kafka-server-start.sh config/server.properties
1.4 Create a topic for the assignment: bin/kafka-topics.sh --create --topic emo --bootstrap-server localhost:9092
1.5 Producer: bin/kafka-console-producer.sh --topic emo --bootstrap-server localhost:9092
1.6 Consumer: bin/kafka-console-consumer.sh --topic emo --from-beginning --bootstrap-server localhost:9092

2. We have start the ELK stack
2.1.1 Go to the directory of ElasticSearch
2.1.2 ./bin/elasticsearch

2.2.1 Go to the directory of Kibana
2.2.2 ./bin/kibana

2.3.1 Go to the directory of LogStash
2.3.2 bin/logstash -f logstash.conf


3. Update the logstash config file using the following code:


input {
  kafka {
    bootstrap_servers => "0.0.0.0:9092"
    topics => ["olympics_sentiment"]
    codec => "json"
    decorate_events => true
  }
}
filter {
  mutate {
  convert => {"prediction" => "integer"}
  copy => { "[@metadata][kafka][key]" => "tweet_text" }
  }
}
output {
  elasticsearch {
  hosts => ["0.0.0.0:9200"]
  index => "olympics_sentiment_analysis"
  workers => 1
  }
}


4. Restart Logstash to apply config file changes sudo systemctl restart logstash

5. Run the file twitter_stream_code.py using python3 twitter_stream_code.py

6. From the code folder run the twitter_sentiment_analysis.py file by using the following command

    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 twitter_sentiment_analysis.py  localhost:9092 subscribe <topic_name> <path_to_tweets_csv_file>

    Eg: topic_name : emo
        path_to_tweets_csv_file : "/Users/jp/Downloads/Tweets.csv"

7. Goto the Kibana server by entering http://localhost:5601/ 
    Then goto Management (settings icon on bottom left) Click on Data View under the Kibana section then click create a new index. Here you may be able to see the index name olympics_sentiment_analysis below. Enter the same term in the text input and click next. Then include timestamp and finish. The index is created.

8. Create kibana dashboard based on the streaming data from the tweet_sentiment_analysis code.


