package com.example.orderpayment.order;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * Minimal REST entry point to trigger the flow without Kafka tooling.
 *
 * <p>Example:
 * <pre>
 *   curl -X POST http://localhost:8080/orders \
 *     -H 'Content-Type: application/json' \
 *     -d '{"orderId":"order-123","amount":5000}'
 * </pre>
 */
@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void create(@RequestBody OrderRequest request) {
        orderService.placeOrder(request.orderId(), request.amount());
    }

    public record OrderRequest(String orderId, long amount) {
    }
}
