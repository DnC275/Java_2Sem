package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * Строка
 */
public class RespBulkString implements RespObject {
    private final byte[] data;

    /**
     * Код объекта
     */
    public static final byte CODE = '$';

    public static final int NULL_STRING_SIZE = -1;

    public RespBulkString(byte[] data) {
        this.data = data;
    }

    /**
     * Ошибка ли это? Ответ - нет
     *
     * @return false
     */
    @Override
    public boolean isError() {
        return false;
    }

    /**
     * Строковое представление
     *
     * @return строку, если данные есть. Если нет - null
     */
    @Override
    public String asString() {
        return new String(data);
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        os.write(data.length);
        os.write(CRLF);
        os.write(data);
        os.write(CRLF);
    }
}
