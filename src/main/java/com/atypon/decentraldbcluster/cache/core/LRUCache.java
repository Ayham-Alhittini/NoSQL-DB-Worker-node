package com.atypon.decentraldbcluster.cache.core;

import java.util.HashMap;
import java.util.Map;

public class LRUCache<Key, Value> implements Cache<Key, Value> {
    private static final int DEFAULT_CAPACITY = 100;
    private final int capacity;
    private final Map<Key, Node<Key, Value>> cache = new HashMap<>();
    private Node<Key, Value> head, tail;



    public LRUCache() {
        this(DEFAULT_CAPACITY);
    }

    public LRUCache(int capacity) {
        this.capacity = capacity;
    }

    @Override
    public synchronized Value get(Key key) {
        if (!cache.containsKey(key)) {
            return null;
        }
        Node<Key, Value> node = cache.get(key);
        moveToHead(node);
        return node.value;
    }

    @Override
    public synchronized void put(Key key, Value value) {
        Node<Key, Value> node = cache.get(key);
        if (node == null) {
            Node<Key, Value> newNode = new Node<>(key, value);
            cache.put(key, newNode);
            addNode(newNode);
            if (cache.size() > capacity) {
                cache.remove(removeTail().key);
            }
        } else {
            node.value = value;
            moveToHead(node);
        }
    }

    @Override
    public synchronized void remove(Key key) {
        Node<Key, Value> node = cache.get(key);
        if (node != null) {
            removeNode(node);
            cache.remove(key);
        }
    }

    @Override
    public synchronized boolean containsKey(Key key) {
        return cache.containsKey(key);
    }

    private void addNode(Node<Key, Value> node) {
        node.next = head;
        node.prev = null;
        if (head != null) {
            head.prev = node;
        }
        head = node;
        if (tail == null) {
            tail = node;
        }
    }

    private void removeNode(Node<Key, Value> node) {
        if (node.prev != null) {
            node.prev.next = node.next;
        } else {
            head = node.next;
        }
        if (node.next != null) {
            node.next.prev = node.prev;
        } else {
            tail = node.prev;
        }
    }

    private void moveToHead(Node<Key, Value> node) {
        removeNode(node);
        addNode(node);
    }

    private Node<Key, Value> removeTail() {
        Node<Key, Value> res = tail;
        removeNode(res);
        return res;
    }

    private static class Node<Key, Value> {
        Key key;
        Value value;
        Node<Key, Value> prev, next;

        Node(Key key, Value value) {
            this.key = key;
            this.value = value;
        }
    }
}
