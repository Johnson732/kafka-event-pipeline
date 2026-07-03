package com.kafka.kafkaeventpipeline.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;

@Service
public class AnalyticsConsumer {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsConsumer.class);

    private final AtomicInteger orderCount = new AtomicInteger(0);

    @KafkaListener(
            topics = "${topic.order}",
            groupId = "analytics-group"
    )
    public void consume(ConsumerRecord<String, String> record) {
        int count = orderCount.incrementAndGet();
        log.info("Analytics → partition={}, offset={}, totalOrdersSeen={}",
                record.partition(),
                record.offset(),
                count);
    }
}