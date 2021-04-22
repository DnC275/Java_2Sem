package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;

import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import com.itmo.java.basics.initialization.TableInitializationContext;

import java.nio.file.Path;
import java.util.Optional;

public class TableImpl implements Table {
    private String name;
    private Path path;
    private TableIndex index;
    private Segment actualSegment = null;

    private TableImpl(String name, Path path, TableIndex index){
        this.name = name;
        this.path = path;
        this.index = index;
    }

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        if (!(new File(pathToDatabaseRoot.toString())).exists()) {
            throw new DatabaseException(String.format("Failed to create a table by path \"%s\"", pathToDatabaseRoot));
        }
        Path fullPath = FileSystems.getDefault().getPath(pathToDatabaseRoot.toString(), tableName);
        File file = new File(fullPath.toString());
        if (!file.mkdir()) {
            throw new DatabaseException(String.format("Failed to create a table by path \"%s\"", pathToDatabaseRoot));
        }
        return new CachingTable(new TableImpl(tableName, fullPath, tableIndex));
    }

    public static Table initializeFromContext(TableInitializationContext context) {
        return null;
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
            throw new DatabaseException(String.format("IO exception when writing to table \"%s\" by path \"%s\"", name, path), ex);
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        if (index.searchForKey(objectKey).isPresent()){
            try{
                return index.searchForKey(objectKey).get().read(objectKey);
            } catch (IOException ex){
                throw new DatabaseException(String.format("IO exception when reading from table \"%s\" by path \"%s\"", name, path), ex);
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
            throw new DatabaseException(String.format("IO exception when writing to table \"%s\" by path \"%s\"", name, path), ex);
        }
    }
}
