package com.itmo.java.protocol;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class RespReader implements AutoCloseable {

    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    private PushbackInputStream pushbackInputStream;

    public RespReader(InputStream is) {
        pushbackInputStream = new PushbackInputStream(is);
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
            case RespError.CODE: {
                return readError();
            }
            case RespBulkString.CODE: {
                return readBulkString();
            }
            case RespArray.CODE: {
                return readArray();
            }
            case RespCommandId.CODE: {
                return readCommandId();
            }
            default: {
                throw new IOException("Unknown Object"); //TODO
            }
        }
    }

    /**
     * Считывает объект ошибки
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespError readError() throws IOException {
        byte type = (byte) pushbackInputStream.read();
        if (type != RespError.CODE)
            throw new IOException("Error syntax error");
        return new RespError(readToCRLF());
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
            throw new IOException("Bulk String syntax error");
        int length = Integer.parseInt(new String(readToCRLF()));
        if (length == -1)
            return RespBulkString.NULL_STRING;
        byte[] message = pushbackInputStream.readNBytes(length);
        byte cr = (byte) pushbackInputStream.read();
        byte lf = (byte) pushbackInputStream.read();
        if (cr != CR || lf != LF)
            throw new IOException("Bulk String syntax error");
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
            throw new IOException("Array syntax error");
        int count = Integer.parseInt(new String(readToCRLF()));
        if (count < 1) {
            throw new IOException("");
        }
        RespObject[] objects = new RespObject[count];
        for (int i = 0; i < count; i++){
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
            throw new IOException("Command id syntax error");
        byte[] byteId = readToCRLF();
        ByteBuffer bb = ByteBuffer.wrap(byteId);
        return new RespCommandId(bb.getInt());
    }


    @Override
    public void close() throws IOException {
        pushbackInputStream.close();
    }

    private byte[] readToCRLF() throws IOException {
        List<Byte> message = new ArrayList<>();
        byte b = (byte) pushbackInputStream.read();
        while (true) {
            if (b == -1) {
                throw new IOException("Unexpected end of stream");
            }
            if (b != CR) {
                message.add(b);
                b = (byte) pushbackInputStream.read();
            }
            else {
                b = (byte) pushbackInputStream.read();
                if (b == LF) {
                    break;
                }
                message.add(b);
            }
        }
        byte[] result = new byte[message.size()];
        int i = 0;
        for (Byte character : message)
            result[i++] = character;
        return result;
    }

    private byte getNextByte() throws IOException {
        try {
            byte firstByte = (byte) pushbackInputStream.read();
            pushbackInputStream.unread(firstByte);
            return firstByte;
        }
        catch (EOFException e) {
            throw e;
        }
        catch (IOException e) {
            throw new IOException("Error", e);
        }
    }
}