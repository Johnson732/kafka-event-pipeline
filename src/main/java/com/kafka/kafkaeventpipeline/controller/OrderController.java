package com.kafka.kafkaeventpipeline.controller;

import com.kafka.kafkaeventpipeline.model.OrderEvent;
import com.kafka.kafkaeventpipeline.producer.OrderEventProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderEventProducer producer;

    public OrderController(OrderEventProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    public ResponseEntity<Map<String, String>> createOrder(@RequestBody OrderRequest request) {
        OrderEvent event = OrderEvent.create(
                request.userId(),
                request.productId(),
                request.quantity(),
                request.totalAmount()
        );
        producer.sendOrderEvent(event);
        return ResponseEntity.ok(Map.of(
                "orderId", event.orderId(),
                "status", "published to Kafka"
        ));
    }

    @PostMapping("/test")
    public ResponseEntity<Map<String, String>> testOrder() {
        OrderEvent event = OrderEvent.create("user-123", "product-456", 2, 199.99);
        producer.sendOrderEvent(event);
        return ResponseEntity.ok(Map.of(
                "orderId", event.orderId(),
                "status", "test event published"
        ));
    }

    public record OrderRequest(
            String userId,
            String productId,
            int quantity,
            double totalAmount
    ) {}
}