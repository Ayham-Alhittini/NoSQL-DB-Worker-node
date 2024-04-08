package com.atypon.decentraldbcluster.utility;

public class DocumentUtil {
    public static int extractNodePortFromDocumentId(String documentId) {
        int basePort = 8080;
        // The last digit in the ID represent the node number
        int nodeNumber = documentId.charAt(documentId.length() - 1) - '0';
        return basePort + nodeNumber;
    }
}
