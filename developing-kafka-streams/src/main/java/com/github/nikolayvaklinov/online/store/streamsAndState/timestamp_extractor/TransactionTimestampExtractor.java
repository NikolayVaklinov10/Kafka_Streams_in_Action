package com.github.nikolayvaklinov.online.store.streamsAndState.timestamp_extractor;

import com.github.nikolayvaklinov.online.store.yellingApp.producer.model.Purchase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class TransactionTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        Purchase purchasePurchaseTransaction = (Purchase) record.value();
        return purchasePurchaseTransaction.getPurchaseDate().getTime();
    }
}
