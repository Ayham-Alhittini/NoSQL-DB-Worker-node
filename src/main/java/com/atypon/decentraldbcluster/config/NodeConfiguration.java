package com.atypon.decentraldbcluster.config;

import java.util.ArrayList;
import java.util.List;

public class NodeConfiguration {

    private static int currentNodePort;
    private static List<Integer> otherNodesPort;


    public static String getNodeAddress(int portNumber) {
        return "http://host.docker.internal:" + portNumber;
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
