package com.kafka.kafkaeventpipeline.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderEventConsumer2 {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer2.class);

    @KafkaListener(
            topics = "${topic.order}",
            groupId = "order-processor-group"
    )
    public void consume(ConsumerRecord<String, String> record) {
        log.info("Consumer-2 Consumed → partition={}, offset={}, key={}",
                record.partition(),
                record.offset(),
                record.key());
    }
}