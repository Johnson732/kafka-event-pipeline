package com.kafka.kafkaeventpipeline.producer;

import com.kafka.kafkaeventpipeline.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${topic.order}")
    private String orderTopic;

    public OrderEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrderEvent(OrderEvent event) {
        String payload = String.format(
                "{\"orderId\":\"%s\",\"userId\":\"%s\",\"productId\":\"%s\"," +
                        "\"quantity\":%d,\"totalAmount\":%.2f,\"status\":\"%s\",\"timestamp\":\"%s\"}",
                event.orderId(), event.userId(), event.productId(),
                event.quantity(), event.totalAmount(), event.status(), event.timestamp()
        );
        CompletableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(orderTopic, event.orderId(), payload);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send order event: orderId={}, error={}",
                        event.orderId(), ex.getMessage());
            } else {
                log.info("Order sent → topic={}, partition={}, offset={}, orderId={}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        event.orderId());
            }
        });
    }
}