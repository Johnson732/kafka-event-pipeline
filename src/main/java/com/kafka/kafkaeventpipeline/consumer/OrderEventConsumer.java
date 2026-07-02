package com.kafka.kafkaeventpipeline.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Map;

@Service
public class OrderEventConsumer implements ConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    @KafkaListener(
            topics = "${topic.order}",
            groupId = "order-processor-group"
    )
    public void consume(ConsumerRecord<String, String> record) {
        log.info("Consumed → partition={}, offset={}, key={}, value={}",
                record.partition(),
                record.offset(),
                record.key(),
                record.value());
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments,
                                     ConsumerSeekCallback callback) {
        log.info("Partitions assigned: {}", assignments.keySet());
    }
}