package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

public class SetDatabaseRecord implements WritableDatabaseRecord {
    private static final int REMOVED_OBJECT_SIZE = -1;
    private byte[] key;
    private byte[] value;

    public SetDatabaseRecord(byte[] key, byte[] value){
        this.key = key;
        this.value = value;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public long size() {
        return key.length + value.length + 8;
    }

    @Override
    public boolean isValuePresented() {
        return true;
    }

    @Override
    public int getKeySize() {
        return key.length;
    }

    @Override
    public int getValueSize() {
        if (value != null) {
            return value.length;
        }
        return REMOVED_OBJECT_SIZE;
    }
}
