package com.atypon.decentraldbcluster.api;

import com.atypon.decentraldbcluster.communication.config.NodeCommunicationConfiguration;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/internal/api/bootstrap")
@CrossOrigin("*")
public class BootstrapController {


    //Todo: consider moving create database into here

    @PostMapping("/initializeNode")
    public void initializeNode(@RequestBody JsonNode initialConfiguration) {
        NodeCommunicationConfiguration.setCurrentNodePort(initialConfiguration.get("currentNodePort").asInt());

        List<Integer> otherPortsNumber = getPortsNumber(initialConfiguration.get("otherNodesPort"));
        NodeCommunicationConfiguration.setOtherNodesPort(otherPortsNumber);
    }

    private List<Integer> getPortsNumber(JsonNode arrayNode) {
        List<Integer> ports = new ArrayList<>();

        if (arrayNode.isArray())
            for (JsonNode port: arrayNode)
                ports.add(port.asInt());

        return ports;
    }

}
