package com.atypon.decentraldbcluster.communication.config;

import java.util.ArrayList;
import java.util.List;

public class NodeCommunicationConfiguration {

    //Production
//    private static final String baseNodeAddress = "http://host.docker.internal:";
//    private static int currentNodePort;
//    private static List<Integer> otherNodesPort;

    //Development
    private static final String baseNodeAddress = "http://localhost:";
    private static int currentNodePort = 8081;
    private static List<Integer> otherNodesPort = List.of(8082);
//    private static List<Integer> otherNodesPort = new ArrayList<>();

    public static int getClusterNodeSize() {
        return otherNodesPort.size() + 1;
    }

    public static String getNodeAddress(int portNumber) {
        return baseNodeAddress + portNumber;
    }

    public static int getCurrentNodePort() {
        return currentNodePort;
    }

    public static List<Integer> getOtherNodesPort() {
        return otherNodesPort;
    }

    public static void setCurrentNodePort(int currentNodePort) {
        NodeCommunicationConfiguration.currentNodePort = currentNodePort;
    }

    public static void setOtherNodesPort(List<Integer> otherNodesPort) {
        NodeCommunicationConfiguration.otherNodesPort = otherNodesPort;
    }
}
