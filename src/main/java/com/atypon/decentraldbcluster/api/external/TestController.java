package com.atypon.decentraldbcluster.api.external;

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

@RestController
@RequestMapping("/api/test")
@CrossOrigin("*")
public class TestController {

    public TestController() {

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



}
