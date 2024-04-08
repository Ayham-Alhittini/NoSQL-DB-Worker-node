package com.atypon.decentraldbcluster.test.api;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/test")
@CrossOrigin("*")
public class TestController {

    @GetMapping("endpoint1")
    public Object endpoint1() {
        return "Working...";
    }


}
