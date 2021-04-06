package com.github.nikolayvaklinov.online.store.KTableAPI.timestamp_extractor;

import com.github.nikolayvaklinov.online.store.yellingApp.producer.model.StockTransaction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.util.Date;

public class StockTransactionTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {

        if(! (consumerRecord.value() instanceof StockTransaction)) {
            return System.currentTimeMillis();
        }

        StockTransaction stockTransaction = (StockTransaction) consumerRecord.value();
        Date transactionDate = stockTransaction.getTransactionTimestamp();
        return   (transactionDate != null) ? transactionDate.getTime() : consumerRecord.timestamp();
    }
}