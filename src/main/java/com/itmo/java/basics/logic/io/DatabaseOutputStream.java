package com.itmo.java.basics.logic.io;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Записывает данные в БД
 */
public class DatabaseOutputStream extends DataOutputStream {

    public DatabaseOutputStream(OutputStream outputStream) {
        super(outputStream);
    }

    /**
     * Записывает в БД в следующем формате:
     * - Размер ключа в байтах используя {@link WritableDatabaseRecord#getKeySize()}
     * - Ключ
     * - Размер записи в байтах {@link WritableDatabaseRecord#getValueSize()}
     * - Запись
     * Например при использовании UTF_8,
     * "key" : "value"
     * 3key5value
     * Метод вернет 10
     *
     * @param databaseRecord запись
     * @return размер записи
     * @throws IOException если запись не удалась
     */
    //mb int
    public long write(WritableDatabaseRecord databaseRecord) throws IOException {
        write(databaseRecord.getKeySize());
        write(databaseRecord.getKey());
        write(databaseRecord.getValueSize());
        write(databaseRecord.getValue());
        return databaseRecord.size();
    }
}