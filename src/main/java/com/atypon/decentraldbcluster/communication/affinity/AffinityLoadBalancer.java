package com.atypon.decentraldbcluster.communication.affinity;

import com.atypon.decentraldbcluster.communication.config.NodeCommunicationConfiguration;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;

@Service
public class AffinityLoadBalancer {

    private final AtomicInteger currentNodePointer = new AtomicInteger(0);

    public int getNextNodeNumber() {
        return currentNodePointer.getAndUpdate(nodeNumber -> (nodeNumber + 1) % NodeCommunicationConfiguration.getClusterNodeSize()) + 1;
    }
}
