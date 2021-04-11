package com.github.nikolayvaklinov.online.store.Monitoring;

import com.github.nikolayvaklinov.online.store.Monitoring.interceptors.ZMartProducerInterceptor;
import com.github.nikolayvaklinov.online.store.yellingApp.producer.model.Purchase;
import com.github.nikolayvaklinov.online.store.yellingApp.producer.model.PurchasePattern;
import com.github.nikolayvaklinov.online.store.yellingApp.producer.model.RewardAccumulator;
import com.github.nikolayvaklinov.online.store.yellingApp.producer.util.MockDataProducer;
import com.github.nikolayvaklinov.online.store.yellingApp.producer.util.datagen.DataGenerator;
import com.github.nikolayvaklinov.online.store.yellingApp.producer.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@SuppressWarnings("unchecked")
public class ZMartKafkaStreamsAdvancedReqsMetricsApp {

    private static final Logger LOG = LoggerFactory.getLogger(ZMartKafkaStreamsAdvancedReqsMetricsApp.class);

    public static void main(String[] args) throws Exception {

        StreamsConfig streamsConfig = new StreamsConfig(getProperties());

        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder streamsBuilder = new StreamsBuilder();


        /**
         * Previous requirements
         */
        KStream<String,Purchase> purchaseKStream = streamsBuilder.stream("transactions", Consumed.with(stringSerde, purchaseSerde))
                .mapValues(p -> Purchase.builder(p).maskCreditCard().build());

        KStream<String, PurchasePattern> patternKStream = purchaseKStream.mapValues(purchase -> PurchasePattern.builder(purchase).build());

        patternKStream.to("patterns", Produced.with(stringSerde,purchasePatternSerde));


        KStream<String, RewardAccumulator> rewardsKStream = purchaseKStream.mapValues(purchase -> RewardAccumulator.builder(purchase).build());

        rewardsKStream.to("rewards", Produced.with(stringSerde,rewardAccumulatorSerde));


        /**
         *  Selecting a key for storage and filtering out low dollar purchases
         */

        KeyValueMapper<String, Purchase, Long> purchaseDateAsKey = (key, purchase) -> purchase.getPurchaseDate().getTime();

        KStream<Long, Purchase> filteredKStream = purchaseKStream.filter((key, purchase) -> purchase.getPrice() > 5.00).selectKey(purchaseDateAsKey);

        filteredKStream.to("purchases", Produced.with(Serdes.Long(),purchaseSerde));


        /**
         * Branching stream for separating out purchases in new departments to their own topics
         */
        Predicate<String, Purchase> isCoffee = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("coffee");
        Predicate<String, Purchase> isElectronics = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("electronics");

        int coffee = 0;
        int electronics = 1;

        KStream<String, Purchase>[] kstreamByDept = purchaseKStream.branch(isCoffee, isElectronics);

        kstreamByDept[coffee].to("coffee", Produced.with(stringSerde, purchaseSerde));

        kstreamByDept[electronics].to("electronics", Produced.with(stringSerde, purchaseSerde));



        /**
         * Security Requirements to record transactions for certain employee
         */
        ForeachAction<String, Purchase> purchaseForeachAction = (key, purchase) -> { };


        purchaseKStream.filter((key, purchase) -> purchase.getEmployeeId().equals("000000")).foreach(purchaseForeachAction);

        Topology topology = streamsBuilder.build();


        KafkaStreams kafkaStreams = new KafkaStreams(topology, getProperties());

        KafkaStreams.StateListener stateListener = (newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
                LOG.info("Application has gone from REBALANCING to RUNNING ");
                LOG.info("Topology Layout {}", streamsBuilder.build().describe());
            }

            if (newState == KafkaStreams.State.REBALANCING) {
                LOG.info("Application is entering REBALANCING phase");
            }
        };

        kafkaStreams.setStateListener(stateListener);
        LOG.info("ZMart Advanced Requirements Metrics Application Started");
        kafkaStreams.cleanUp();
        CountDownLatch stopSignal = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            LOG.info("Shutting down the Kafka Streams Application now");
            kafkaStreams.close();
            MockDataProducer.shutdown();
            stopSignal.countDown();
        }));



        MockDataProducer.producePurchaseData(DataGenerator.DEFAULT_NUM_PURCHASES, 250, DataGenerator.NUMBER_UNIQUE_CUSTOMERS);
        kafkaStreams.start();

        stopSignal.await();
        LOG.info("All done now, good-bye");
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "zmart-metrics-client-id");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "zmart-metrics-group-id");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "zmart-metrics-application-id");
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.producerPrefix(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG), Collections.singletonList(ZMartProducerInterceptor.class));
        return props;
    }

}