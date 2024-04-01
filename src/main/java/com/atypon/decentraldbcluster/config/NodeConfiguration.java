package com.atypon.decentraldbcluster.config;

import java.util.ArrayList;
import java.util.List;

public class NodeConfiguration {

    //Production
//    private static final String baseNodeAddress = "http://host.docker.internal:";
//    private static int currentNodePort;
//    private static List<Integer> otherNodesPort;

    //Development
    private static final String baseNodeAddress = "http://localhost:";

    private static int currentNodePort = 8081;
    private static List<Integer> otherNodesPort = List.of(8082);


    public static String getNodeAddress(int portNumber) {
        return baseNodeAddress + portNumber;
    }

    public static List<String> getNodesAddress() {
        List<String> nodesAddress = new ArrayList<>();
        for (Integer portNumber: otherNodesPort) {
            nodesAddress.add( getNodeAddress(portNumber) );
        }
        return nodesAddress;
    }

    public static int getCurrentNodePort() {
        return currentNodePort;
    }
    public static String getCurrentNodeAddress() {
        return getNodeAddress(currentNodePort);
    }

    public static List<Integer> getOtherNodesPort() {
        return otherNodesPort;
    }

    public static void setCurrentNodePort(int currentNodePort) {
        NodeConfiguration.currentNodePort = currentNodePort;
    }

    public static void setOtherNodesPort(List<Integer> otherNodesPort) {
        NodeConfiguration.otherNodesPort = otherNodesPort;
    }
}