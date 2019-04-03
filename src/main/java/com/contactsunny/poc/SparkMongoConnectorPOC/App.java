package com.contactsunny.poc.SparkMongoConnectorPOC;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootApplication
public class App implements CommandLineRunner {

    private static final Logger logger = Logger.getLogger(App.class);

    @Value("${spring.data.mongodb.uri}")
    private String mongoDbConnectionUri;

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }

    @Override
    public void run(String... strings) throws Exception {

        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .set("spark.mongodb.input.uri", mongoDbConnectionUri + ".tweets")
                .setAppName("SparkMongoConnectorPOC");

        Map<String, String> tweetsReadOverrides = new HashMap<>();
        tweetsReadOverrides.put("collection", "tweets");
        tweetsReadOverrides.put("readPreference.name", "secondaryPreferred");
        ReadConfig tweetsReadConfig = ReadConfig.create(sparkConf).withOptions(tweetsReadOverrides);

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaMongoRDD<Document> tweetsRdd = MongoSpark.load(javaSparkContext, tweetsReadConfig);

        String matchQuery = "\"user.id_str\": \"150820027\"";

        JavaRDD<Document> tweetsJavaRdd = tweetsRdd.withPipeline(Collections.singletonList(
                Document.parse("{ $match: { " + matchQuery + " } }")
        ));

        JavaRDD<String> tweetTexts = tweetsJavaRdd.map(singleRdd -> {
            return singleRdd.getString("text");
        });

        List<String> tweets = tweetTexts.collect();
        long tweetsCount = tweetTexts.count();

        logger.info("Collected tweets: " + tweets);
        logger.info("Collected tweets count: " + tweetsCount);
    }
}
