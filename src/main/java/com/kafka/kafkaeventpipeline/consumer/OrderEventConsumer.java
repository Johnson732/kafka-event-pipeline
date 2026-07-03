package com.kafka.kafkaeventpipeline.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);
    private int orderCount = 0;

    @KafkaListener(
            topics = "${topic.order}",
            groupId = "order-processor-group"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            log.info("Processing → partition={}, offset={}, key={}",
                    record.partition(),
                    record.offset(),
                    record.key());

            processOrder(record.value());

            ack.acknowledge();
            log.info("Committed → partition={}, offset={}",
                    record.partition(),
                    record.offset());

        } catch (Exception e) {
            log.error("Failed → partition={}, offset={}, error={}",
                    record.partition(),
                    record.offset(),
                    e.getMessage());
        }
    }

    private void processOrder(String value) throws Exception {
//        if (orderCount++ % 3 == 0) {
//            throw new RuntimeException("Simulated crash during processing!");
//        }
        log.info("Doing business logic for order...");
    }
}