package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

public class RemoveDatabaseRecord implements WritableDatabaseRecord {
    private static final int REMOVED_OBJECT_SIZE = -1;
    byte[] key;

    public RemoveDatabaseRecord(byte[] key){
        this.key = key;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return new byte[0];
    }

    @Override
    public long size() {
        return key.length + 8;
    }

    @Override
    public boolean isValuePresented() {
        return false;
    }

    @Override
    public int getKeySize() {
        return key.length;
    }

    @Override
    public int getValueSize() {
        return REMOVED_OBJECT_SIZE;
    }
}
