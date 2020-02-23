package com.kep.kafka.twitter;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterClient {

  public String apiKey = System.getProperty("API_KEY");
  public String apiSecretKey = System.getProperty("API_SECRET_KEY");
  public String accessToken = System.getProperty("ACCESS_TOKEN");
  public String accessTokenSecret = System.getProperty("ACCESS_TOKEN_SECRET");

  private Logger logger = LoggerFactory.getLogger(TwitterClient.class);
  private BlockingQueue<String> msgQueue;
  private Client twitterClient;

  public TwitterClient(String... terms) {
    msgQueue = new LinkedBlockingQueue<>(100000);
    twitterClient = createTwitterClient(msgQueue, Arrays.asList(terms));

    addShutdownHook();
  }

  public void getTweets(Consumer<String> callback) {
    twitterClient.connect();
    try {
      while (!twitterClient.isDone()) {
        callback.accept(msgQueue.take());
      }
    } catch (InterruptedException e) {
      logger.error("Error fetching tweets", e);
    } finally {
      twitterClient.stop();
    }
  }

  private Client createTwitterClient(BlockingQueue<String> msgQueue, List<String> terms) {
    Hosts hbHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hbEndpoint = new StatusesFilterEndpoint();

    hbEndpoint.trackTerms(terms);

    Authentication hbAuth = new OAuth1(apiKey, apiSecretKey, accessToken, accessTokenSecret);

    ClientBuilder builder =
        new ClientBuilder().name("HB-Client-01").hosts(hbHosts).authentication(hbAuth).endpoint(hbEndpoint).processor(new StringDelimitedProcessor(msgQueue));

    return builder.build();
  }

  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Shutting down twitter client");
      twitterClient.stop();
      logger.info("Done shutting down twitter client");
    }));
  }

}
