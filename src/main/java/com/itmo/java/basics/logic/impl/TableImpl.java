package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.KvsIndex;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Optional;

public class TableImpl implements Table {
    String name;
    Path path;
//    KvsIndex index;
    TableIndex index;
    Segment actualSegment = null;

    private TableImpl(String name, Path path, TableIndex index){
        this.name = name;
        this.path = path;
        this.index = index;
    }

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        if (!(new File(pathToDatabaseRoot.toString())).exists()){
            throw new DatabaseException("The specified database path does not exist");
        }
        Path fullPath = FileSystems.getDefault().getPath(pathToDatabaseRoot.toString(), tableName);
        File file = new File(fullPath.toString());
        file.mkdir();
        return new TableImpl(tableName, fullPath, new TableIndex());
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        try {
            if (actualSegment == null || actualSegment.isReadOnly()) {
                actualSegment = SegmentImpl.create(SegmentImpl.createSegmentName(name), path);
            }
            actualSegment.write(objectKey, objectValue);
            index.onIndexedEntityUpdated(objectKey, actualSegment);
        } catch (IOException ex){
            throw new DatabaseException(ex);
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        if (index.searchForKey(objectKey).isPresent()){
            try{
                Optional<byte[]> objectValue = index.searchForKey(objectKey).get().read(objectKey);
                return objectValue;
            } catch (IOException ex){
                throw new DatabaseException(ex);
            }
        }
        return Optional.empty();
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        try {
            if (actualSegment == null || actualSegment.isReadOnly()) {
                actualSegment = SegmentImpl.create(SegmentImpl.createSegmentName(name), path);
            }
            actualSegment.delete(objectKey);
            index.onIndexedEntityUpdated(objectKey, actualSegment);
        } catch (IOException ex){
            throw new DatabaseException(ex);
        }
    }
}
