package com.atypon.decentraldbcluster.api.internal;

import com.atypon.decentraldbcluster.affinity.AffinityLoadBalancer;
import com.atypon.decentraldbcluster.config.NodeConfiguration;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/internal/api/bootstrap")
@CrossOrigin("*")
public class BootstrapController {
    private final AffinityLoadBalancer affinityLoadBalancer;

    @Autowired
    public BootstrapController(AffinityLoadBalancer affinityLoadBalancer) {
        this.affinityLoadBalancer = affinityLoadBalancer;
    }

    @PostMapping("/initializeNode")
    public void initializeNode(@RequestBody JsonNode initialConfiguration) {
        NodeConfiguration.setCurrentNodePort(initialConfiguration.get("currentNodePort").asInt());

        List<Integer> otherPortsNumber = getPortsNumber(initialConfiguration.get("otherNodesPort"));
        NodeConfiguration.setOtherNodesPort(otherPortsNumber);
        affinityLoadBalancer.initializeAffinityNodes();
    }

    private List<Integer> getPortsNumber(JsonNode arrayNode) {
        List<Integer> ports = new ArrayList<>();

        if (arrayNode.isArray())
            for (JsonNode port: arrayNode)
                ports.add(port.asInt());

        return ports;
    }

}
