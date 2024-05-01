package com.atypon.decentraldbcluster.api;

import com.atypon.decentraldbcluster.communication.config.NodeCommunicationConfiguration;
import com.atypon.decentraldbcluster.utility.BootstrapUtil;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.web.bind.annotation.*;

import java.util.List;


@RestController
@RequestMapping("/internal/api/bootstrap")
public class BootstrapController {

    @PostMapping("/initializeNode")
    public void initializeNode(@RequestBody JsonNode initialConfiguration) {
        NodeCommunicationConfiguration.setCurrentNodePort(initialConfiguration.get("currentNodePort").asInt());

        List<Integer> otherPortsNumber = BootstrapUtil.getPortsNumber(initialConfiguration.get("otherNodesPort"));
        NodeCommunicationConfiguration.setOtherNodesPort(otherPortsNumber);
    }
}
