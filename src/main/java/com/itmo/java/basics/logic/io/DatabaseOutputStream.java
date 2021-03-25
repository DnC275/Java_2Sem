package com.itmo.java.basics.logic.io;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

import java.io.*;

public class DatabaseOutputStream extends DataOutputStream {

    public DatabaseOutputStream(OutputStream outputStream) {
        super(outputStream);
    }

    public long write(WritableDatabaseRecord databaseRecord) throws IOException {
        writeInt(databaseRecord.getKeySize());
        write(databaseRecord.getKey());
        writeInt(databaseRecord.getValueSize());
        write(databaseRecord.getValue());
        return databaseRecord.size();
    }
}