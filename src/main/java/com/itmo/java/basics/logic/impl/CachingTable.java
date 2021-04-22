package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Table;

import java.util.Optional;

public class CachingTable implements Table {
    Table table;

    public CachingTable(Table table){
        this.table = table;
    }

    @Override
    public String getName(){
        return table.getName();
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        //TODO caching
        table.write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        //TODO caching
        return table.read(objectKey);
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        //TODO delete from cache
        table.delete(objectKey);
    }
}
