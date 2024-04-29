package com.atypon.decentraldbcluster.communication.affinity.balancer;

import com.atypon.decentraldbcluster.communication.config.NodeCommunicationConfiguration;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;

@Service
public class RoundRobinLoadBalancer implements AffinityLoadBalancer {

    private final AtomicInteger currentNodePointer = new AtomicInteger(0);

    @Override
    public int getNextAffinityNodeNumber() {
        return currentNodePointer.getAndUpdate(nodeNumber -> (nodeNumber + 1) % NodeCommunicationConfiguration.getClusterNodeSize()) + 1;
    }
}
