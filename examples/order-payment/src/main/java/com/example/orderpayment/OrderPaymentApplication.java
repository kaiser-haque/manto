package com.example.orderpayment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Order-Payment example application.
 *
 * <p>Flow:
 * <pre>
 *   [REST /orders] -> OrderService --publish--> order-events
 *       -> PaymentHandler (@MantoListener) --publish--> payment-events
 *                                    |
 *                              retry + DLT + idempotency
 * </pre>
 */
@SpringBootApplication
public class OrderPaymentApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderPaymentApplication.class, args);
    }
}
