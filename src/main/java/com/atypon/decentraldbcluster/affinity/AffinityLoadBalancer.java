package com.atypon.decentraldbcluster.affinity;

import com.atypon.decentraldbcluster.config.NodeConfiguration;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;

@Service
public class AffinityLoadBalancer {

    private final AtomicInteger currentNodePointer = new AtomicInteger(0);

    public int getNextNodeNumber() {
        return currentNodePointer.getAndUpdate(nodeNumber -> (nodeNumber + 1) % NodeConfiguration.getClusterNodeSize()) + 1;
    }
}
