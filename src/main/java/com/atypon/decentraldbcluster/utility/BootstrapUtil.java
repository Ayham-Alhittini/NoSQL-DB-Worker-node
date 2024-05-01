package com.atypon.decentraldbcluster.utility;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

public class BootstrapUtil {
    public static List<Integer> getPortsNumber(JsonNode arrayNode) {
        List<Integer> ports = new ArrayList<>();

        if (arrayNode.isArray())
            for (JsonNode port: arrayNode)
                ports.add(port.asInt());

        return ports;
    }
}
