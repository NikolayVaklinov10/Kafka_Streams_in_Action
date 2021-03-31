package com.github.nikolayvaklinov.online.store.yellingApp;

import com.github.nikolayvaklinov.online.store.yellingApp.producer.model.Purchase;
import com.github.nikolayvaklinov.online.store.yellingApp.producer.model.PurchasePattern;
import com.github.nikolayvaklinov.online.store.yellingApp.producer.model.RewardAccumulator;
import com.github.nikolayvaklinov.online.store.yellingApp.producer.util.MockDataProducer;
import com.github.nikolayvaklinov.online.store.yellingApp.producer.util.serde.StreamsSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
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
                .mapValues(p -> Purchase.builder(p).maskCreditCard().build());



    }

}

