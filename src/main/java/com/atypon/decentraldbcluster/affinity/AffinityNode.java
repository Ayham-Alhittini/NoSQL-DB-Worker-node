package com.atypon.decentraldbcluster.affinity;

public class AffinityNode implements Comparable<AffinityNode>{

    private final int nodePort;
    private int assignedDocumentsCount;

    public AffinityNode(int nodePort, int assignedDocumentsCount) {
        this.nodePort = nodePort;
        this.assignedDocumentsCount = assignedDocumentsCount;
    }

    public int getNodePort() {
        return nodePort;
    }

    public int getAssignedDocumentsCount() {
        return assignedDocumentsCount;
    }

    public void incrementNodeDocuments() {
        this.assignedDocumentsCount++;
    }
    public void decrementNodeDocuments() {
        this.assignedDocumentsCount--;
    }

    @Override
    public int compareTo(AffinityNode o) {
        if (getAssignedDocumentsCount() == o.getAssignedDocumentsCount())
            return getNodePort() - o.getNodePort();
        return getAssignedDocumentsCount() - o.getAssignedDocumentsCount();
    }
}
