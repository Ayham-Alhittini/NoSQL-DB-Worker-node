package com.atypon.decentraldbcluster.communication.affinity.balancer;

public interface AffinityLoadBalancer {
    int getNextAffinityNodeNumber();
}
