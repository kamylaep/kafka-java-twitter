package com.kep.kafka;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.Gson;
import com.kep.kafka.producer.KafkaEventProducer;
import com.kep.kafka.twitter.TwitterClient;

public class Main {

  public static void main(String[] args) {
    String kafkaOutputTopic = System.getProperty("KAFKA_OUTPUT_TOPIC");
    KafkaEventProducer kafkaEventProducer = new KafkaEventProducer(kafkaOutputTopic);
    TwitterClient twitterClient = new TwitterClient(getTwitterTerms());
    Gson gson = new Gson();

    Consumer<String> consumer = tweet -> {
      Map parsedJson = gson.fromJson(tweet, Map.class);
      kafkaEventProducer.send(Objects.toString(parsedJson.get("id_str")), tweet);
    };

    twitterClient.getTweets(consumer);
  }

  private static String[] getTwitterTerms() {
    String[] twitterTerms = StringUtils.split(System.getProperty("TWITTER_TERMS"), ",");
    return Arrays.stream(twitterTerms).map(StringUtils::trim).toArray(String[]::new);
  }
}
