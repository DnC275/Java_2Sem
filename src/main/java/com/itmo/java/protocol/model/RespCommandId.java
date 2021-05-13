package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Id
 */
public class RespCommandId implements RespObject {
    private final int commandId;

    /**
     * Код объекта
     */
    public static final byte CODE = '!';

    public RespCommandId(int commandId) {
        this.commandId = commandId;
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

    @Override
    public String asString() {
        return Integer.toString(commandId);
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        writeInt(commandId, os);
        os.write(CRLF);
    }

    private void writeInt(int value, OutputStream os) throws IOException{
        os.write(value >>> 24 & 255);
        os.write(value >>> 16 & 255);
        os.write(value >>> 8 & 255);
        os.write(value >>> 0 & 255);
    }
}
