package com.itmo.java.protocol;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RespReader implements AutoCloseable {
    private PushbackInputStream pushbackInputStream;

    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    public RespReader(InputStream is) {
        this.pushbackInputStream = new PushbackInputStream(is);
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {
        return getNextByte() == RespArray.CODE;
    }

    /**
     * Считывает из input stream следующий объект. Может прочитать любой объект, сам определит его тип на основе кода объекта.
     * Например, если первый элемент "-", то вернет ошибку. Если "$" - bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespObject readObject() throws IOException {
        byte b = getNextByte();
        switch (b) {
            case RespCommandId.CODE:
                return readCommandId();
            case RespBulkString.CODE:
                return readBulkString();
            case RespArray.CODE:
                return readArray();
            case RespError.CODE:
                return readError();
            default:
                throw new IOException("Unknown command character");
        }
    }

    /**
     * Считывает объект ошибки
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespError readError() throws IOException {
        byte b = (byte) pushbackInputStream.read();
        if (b != RespError.CODE)
            throw new IOException("The object symbol doesn't match the error symbol");
        byte[] message = readToCRLF(pushbackInputStream);
        return new RespError(message);
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
        byte b = (byte) pushbackInputStream.read();
        if (b != RespBulkString.CODE)
            throw new IOException("The object symbol doesn't match the bulkString symbol");
        int length = Integer.parseInt(new String(readToCRLF(pushbackInputStream)));
        if (length == -1) {
            return RespBulkString.NULL_STRING;
        }
        byte[] message = pushbackInputStream.readNBytes(length);
        byte cr = (byte) pushbackInputStream.read();
        byte lf = (byte) pushbackInputStream.read();
        if (cr != CR || lf != LF) {
            throw new IOException("Invalid bulkString object");
        }
        return new RespBulkString(message);
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
        byte b = (byte) pushbackInputStream.read();
        if (b != RespArray.CODE)
            throw new IOException("The object symbol doesn't match the array symbol");
        int count = Integer.parseInt(new String(readToCRLF(pushbackInputStream)));
        if (count < 1)
            throw new IOException("Invalid array object");
        RespObject[] objects = new RespObject[count];
        for (int i = 0; i < count; i++) {
            objects[i] = readObject();
        }
        return new RespArray(objects);
    }

    /**
     * Считывает id команды
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespCommandId readCommandId() throws IOException {
        byte b = (byte) pushbackInputStream.read();
        if (b != RespCommandId.CODE)
            throw new IOException("The object symbol doesn't match the id symbol");
        byte[] byteId = readToCRLF(pushbackInputStream);
        if (byteId.length != 4)
            throw new IOException("Invalid command id object");
        ByteBuffer bb = ByteBuffer.wrap(byteId);
        return new RespCommandId(bb.getInt());
    }

    @Override
    public void close() throws IOException {
        pushbackInputStream.close();
    }

    private byte getNextByte() throws IOException {
        try {
            byte b = (byte) pushbackInputStream.read();
            if (b == -1) {
                throw new EOFException("Unexpected end of stream");
            }
            pushbackInputStream.unread(b);
            return b;
        }
        catch (EOFException e) {
            throw e;
        }
        catch (IOException e) {
            throw new IOException("Error reading the following character", e);
        }
    }

    public static byte[] readToCRLF(InputStream is) throws IOException {
        try {
            List<Byte> message = new ArrayList<>();
            byte b = (byte) is.read();
            while (true) {
                if (b == -1)
                    throw new EOFException("Unexpected end of stream");
                if (b != CR) {
                    message.add(b);
                    b = (byte) is.read();
                }
                else {
                    b = (byte) is.read();
                    if (b == LF) {
                        break;
                    }
                    message.add(CR);
                }
            }
            byte[] result = new byte[message.size()];
            int i = 0;
            for (Byte character : message)
                result[i++] = character;
            return result;
        }
        catch (EOFException e) {
            throw e;
        }
        catch (IOException e) {
            throw new IOException("Error reading the object", e);
        }
    }
}
