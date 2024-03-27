package com.atypon.decentraldbcluster.services.indexing;

import com.atypon.decentraldbcluster.index.Index;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;

import java.io.*;

@Service
public class IndexManager {

    public Index loadIndex(String indexPath) throws IOException, ClassNotFoundException {
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(indexPath))) {
            return (Index) in.readObject();
        } catch (FileNotFoundException e) {
            return new Index(); // Return a new index if file does not exist
        }
    }

    public void saveIndex(Index index, String indexPath) throws IOException {
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(indexPath))) {
            out.writeObject(index);
        }
    }

    public void addToIndex(String indexPath, JsonNode key, String valuePath) throws Exception {
        Index index = loadIndex(indexPath);
        index.add(key, valuePath);
        saveIndex(index, indexPath);
    }

    public void removeFromIndex(String indexPath, JsonNode key, String valuePath) throws Exception {
        Index index = loadIndex(indexPath);
        index.remove(key, valuePath);
        saveIndex(index, indexPath);
    }
}
