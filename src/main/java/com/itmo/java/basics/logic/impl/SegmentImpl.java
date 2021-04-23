package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.index.KvsIndex;
import com.itmo.java.basics.index.SegmentOffsetInfo;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;
import com.itmo.java.basics.initialization.SegmentInitializationContext;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

public class SegmentImpl implements Segment {
    private String name;
    private Path path;
    private KvsIndex index;
    private boolean readOnly = false;

    private SegmentImpl(String name, Path path, SegmentIndex index) {
        this.name = name;
        this.path = path;
        this.index = index;
    }

    public static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        try {
            Path fullPath = FileSystems.getDefault().getPath(tableRootPath.toString(), segmentName);
            File file = new File(fullPath.toString());
            if (!file.createNewFile()) {
                throw new IOException();
            }
            return new SegmentImpl(segmentName, fullPath, new SegmentIndex());
        } catch (IOException ex) {
            throw new DatabaseException(String.format("Failed to create a segment by path \"%s\"", tableRootPath), ex);
        }
    }

    public static Segment initializeFromContext(SegmentInitializationContext context) {
        return null;
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {
        if (isReadOnly()){
            return false;
        }
        if (objectValue == null){
            return delete(objectKey);
        }
        SetDatabaseRecord setDatabaseRecord = new SetDatabaseRecord(objectKey.getBytes(StandardCharsets.UTF_8), objectValue);
        long offset = writeToFile(setDatabaseRecord);
        index.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(offset));
        return true;
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {
        Optional <SegmentOffsetInfo> offsetInfo = index.searchForKey(objectKey);
        if (offsetInfo.isEmpty()){
            return Optional.empty();
        }
        try (DatabaseInputStream inputStream = new DatabaseInputStream(new FileInputStream(path.toString()))) {
            inputStream.skip(offsetInfo.get().getOffset());
            Optional<DatabaseRecord> dbRecord = inputStream.readDbUnit();
            inputStream.close();
            return dbRecord.map(record -> record.getValue());
        }
        catch (IOException ex){
            throw new IOException(String.format("IO exception when reading value by key \"%s\" from file by path \"%s\"", objectKey, path), ex);
        }
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {
        RemoveDatabaseRecord removeDatabaseRecord = new RemoveDatabaseRecord(objectKey.getBytes(StandardCharsets.UTF_8));
        long offset = writeToFile(removeDatabaseRecord);
        index.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(offset));
        return true;
    }

    private long writeToFile(WritableDatabaseRecord databaseRecord) throws IOException{
        try (DatabaseOutputStream outputStream = new DatabaseOutputStream(new FileOutputStream(path.toString(), true))) {
            File file = new File(path.toString());
            long offset = file.length();
            outputStream.write(databaseRecord);
            outputStream.close();
            if(file.length() >= 100000)
            {
                readOnly = true;
            }
            return offset;
        }
        catch (IOException ex){
            throw new IOException(String.format("IO exception when writing to file by path \"%s\"", path), ex);
        }
    }
}
