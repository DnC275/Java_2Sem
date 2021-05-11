package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.DatabaseCache;
import com.itmo.java.basics.logic.Segment;
import java.util.LinkedHashMap;
import java.util.Map;


public class DatabaseCacheImpl extends LinkedHashMap<String, byte[]> implements DatabaseCache {
    private static final int DEFAULT_CAPACITY = 5000;
    private int capacity;

    public DatabaseCacheImpl(){
        super(DEFAULT_CAPACITY);
        this.capacity = DEFAULT_CAPACITY;
    }

    public DatabaseCacheImpl(int capacity){
        super(capacity);
        this.capacity = capacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, byte[]> eldest) {
        return (size() > capacity);
    }

    @Override
    public byte[] get(String key) {
        return super.get(key);
    }

    @Override
    public void set(String key, byte[] value) {
        super.put(key, value);
    }

    @Override
    public void delete(String key) {
        super.remove(key);
    }
}
