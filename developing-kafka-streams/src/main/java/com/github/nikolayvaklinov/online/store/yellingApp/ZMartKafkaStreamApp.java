package com.github.nikolayvaklinov.online.store.yellingApp;

import com.github.nikolayvaklinov.online.store.yellingApp.producer.model.Purchase;
import com.github.nikolayvaklinov.online.store.yellingApp.producer.model.PurchasePattern;
import com.github.nikolayvaklinov.online.store.yellingApp.producer.model.RewardAccumulator;
import com.github.nikolayvaklinov.online.store.yellingApp.producer.util.MockDataProducer;
import com.github.nikolayvaklinov.online.store.yellingApp.producer.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ZMartKafkaStreamApp {

    private static final Logger LOG = LoggerFactory.getLogger(ZMartKafkaStreamApp.class);

    public static void main(String[] args)  throws Exception{

        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();

        // 1. PROCESSOR NODE: From topic with name 'transactions' we are coping the credit cards and masking their digits except last 4
        KStream<String, Purchase> purchaseKStream = builder.stream("transactions", Consumed.with(stringSerde, purchaseSerde))
                .mapValues(p -> Purchase.builder(p).maskCreditCard().build()); // note: the sink is also a processor node

        // 3. PROCESSOR NODE: the Purchase Pattern is a node needed for analytics of the purchase trend amount, date, location etc. of the purchase
        KStream<String, PurchasePattern> patternKStream = purchaseKStream.mapValues(purchase -> PurchasePattern.builder(purchase).build());

        // print is useful for development and monitoring of the code
        patternKStream.print(Printed.<String, PurchasePattern>toSysOut().withLabel("patterns"));

        // 4. PROCESSOR NODE: the fourth node is a sink one of the purchase pattern, writing the data to a topic
        patternKStream.to("patterns", Produced.with(stringSerde, purchasePatternSerde));

        // 5. PROCESSOR NODE: the customer reward accumulator needed by HQ for rewarding loyal customers
        KStream<String, RewardAccumulator> rewardsKStream = purchaseKStream.mapValues(purchase -> RewardAccumulator.builder(purchase).build());

        // another usage of the print method capabilities for monitoring the accuracy of the code
        rewardsKStream.print(Printed.<String, RewardAccumulator>toSysOut().withLabel("rewards"));

        // 6. PROCESSOR NODE: writing the rewards info to a topic named 'rewards'
        rewardsKStream.to("rewards", Produced.with(stringSerde, rewardAccumulatorSerde));

        // purchase is monitored by print method as well
        purchaseKStream.print(Printed.<String, Purchase>toSysOut().withLabel("purchases"));

        // The final 7.PROCESSOR NODE: needed from NoSQL store such as CASSANDRA or ELASTICSEARCH
        purchaseKStream.to("purchases", Produced.with(stringSerde, purchaseSerde));

        // used only to produce data for this application, not typical usage
        MockDataProducer.producePurchaseData();

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), getProperties());
        LOG.info("ZMart First Kafka Streams Application Started");
        kafkaStreams.start();
        Thread.sleep(65000);
        LOG.info("Shutting down the Kafka Streams Application now");
        kafkaStreams.close();
        MockDataProducer.shutdown();



    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "FirstZmart-Kafka-Streams-Client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"zmart-purchases");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "FirstZmart-Kafka-Streams-App");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;


    }

}

