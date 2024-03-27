package com.atypon.decentraldbcluster.api;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

@RestController
@RequestMapping("/api/test")
public class TestController {

    private final CyclicBarrier waiter = new CyclicBarrier(2);
    public TestController() {

    }

    @GetMapping
    public String get() throws BrokenBarrierException, InterruptedException {
        waiter.await();
        return "Hello";
    }

}
