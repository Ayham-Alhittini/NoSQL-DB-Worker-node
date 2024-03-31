package com.atypon.decentraldbcluster.affinity;

import com.atypon.decentraldbcluster.config.NodeConfiguration;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.TreeSet;

@Service
public class AffinityLoadBalancer {

    //TODO: consider more efficient load balance approach
    private final TreeSet<AffinityNode> sortedNodes = new TreeSet<>();
    private final HashMap<Integer, AffinityNode> nodesKey = new HashMap<>();

    public AffinityLoadBalancer() {
//        initializeAffinityNodes();
    }

    public void initializeAffinityNodes() {
        //Add current node
        AffinityNode currentNode = new AffinityNode(NodeConfiguration.getCurrentNodePort(), 0);
        sortedNodes.add( currentNode );
        nodesKey.put(currentNode.getNodePort(), currentNode);

        //Add other nodes
        for (Integer port: NodeConfiguration.getOtherNodesPort()) {
            AffinityNode node = new AffinityNode(port, 0);
            sortedNodes.add( node );
            nodesKey.put(port, node);
        }
    }

    public synchronized int getNextAffinityNodePort() {
        int port = sortedNodes.first().getNodePort();
        incrementNodeAssignedDocuments(port);
        return port;
    }

    public synchronized void incrementNodeAssignedDocuments(int nodePort) {
        AffinityNode node = nodesKey.remove(nodePort);
        sortedNodes.remove(node);

        node.incrementNodeDocuments();

        nodesKey.put(nodePort, node);
        sortedNodes.add(node);
    }

    public synchronized void decrementNodeAssignedDocuments(int nodePort) {
        AffinityNode node = nodesKey.remove(nodePort);
        sortedNodes.remove(node);

        node.decrementNodeDocuments();

        nodesKey.put(nodePort, node);
        sortedNodes.add(node);
    }


}
