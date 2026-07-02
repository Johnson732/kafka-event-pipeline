package com.kafka.kafkaeventpipeline.model;

import java.time.Instant;
import java.util.UUID;

public record OrderEvent(
        String orderId,
        String userId,
        String productId,
        int quantity,
        double totalAmount,
        String status,
        String timestamp
) {
    public static OrderEvent create(String userId, String productId,
                                    int quantity, double amount) {
        return new OrderEvent(
                UUID.randomUUID().toString(),
                userId,
                productId,
                quantity,
                amount,
                "ORDER_CREATED",
                Instant.now().toString()
        );
    }
}
