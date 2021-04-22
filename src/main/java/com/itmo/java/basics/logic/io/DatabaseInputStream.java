package com.itmo.java.basics.logic.io;

import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.impl.SetDatabaseRecord;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

public class DatabaseInputStream extends DataInputStream {
    private static final int REMOVED_OBJECT_SIZE = -1;

    public DatabaseInputStream(InputStream inputStream) {
        super(inputStream);
    }

    public Optional<DatabaseRecord> readDbUnit() throws IOException {
        int keySize = readInt();
        byte[] keyObject = readNBytes(keySize);
        int valueSize = readInt();
        if (valueSize == REMOVED_OBJECT_SIZE){
            return Optional.empty();
        }
        byte[] valueObject = readNBytes(valueSize);
        return Optional.of(new SetDatabaseRecord(keyObject, valueObject));
    }
}
