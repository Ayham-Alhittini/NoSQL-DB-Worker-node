package com.atypon.decentraldbcluster.api;

import com.atypon.decentraldbcluster.affinity.AffinityLoadBalancer;
import com.atypon.decentraldbcluster.config.NodeConfiguration;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

@RestController
@RequestMapping("/api/test")
@CrossOrigin("*")
public class TestController {

    private final AffinityLoadBalancer loadBalancer;
    public TestController(AffinityLoadBalancer loadBalancer) {

        this.loadBalancer = loadBalancer;
    }

    @GetMapping("endpoint1")
    public String endpoint1() {
        return "Endpoint 1 content";
    }

    @GetMapping("endpoint2")
    public String endpoint2(HttpServletRequest request) {
        RestTemplate restTemplate = new RestTemplate();

        String url = "http://localhost:8081/api/test/endpoint1";

        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", request.getHeader("Authorization"));
        HttpEntity<String> requestEntity = new HttpEntity<>(headers);

        return restTemplate
                .exchange(url, HttpMethod.GET, requestEntity, String.class)
                .getBody();

    }
    @GetMapping("endpoint3")
    public int endpoint3() {
        return NodeConfiguration.getCurrentNodePort();
    }
    @GetMapping("endpoint4")
    public List<String> endpoint4() {
        return NodeConfiguration.getNodesAddress();
    }

    CyclicBarrier barrier = new CyclicBarrier(2);
    @GetMapping("endpoint5")
    public int endpoint5() throws BrokenBarrierException, InterruptedException {
        barrier.await();
        return loadBalancer.getNextAffinityNodePort();
    }

    @GetMapping("endpoint6")
    public String endpoint6(HttpServletRequest request) {
        RestTemplate restTemplate = new RestTemplate();

//        String url = "http://localhost:8082/api/test/endpoint1";
        String url = NodeConfiguration.getNodeAddress(8082) + "/api/test/endpoint1";

        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", request.getHeader("Authorization"));
        HttpEntity<String> requestEntity = new HttpEntity<>(headers);

        return restTemplate
                .exchange(url, HttpMethod.GET, requestEntity, String.class)
                .getBody();

    }

}
