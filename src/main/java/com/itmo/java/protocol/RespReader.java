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
//        int ch1 = pushbackInputStream.read();
//        int ch2 = pushbackInputStream.read();
//        int ch3 = pushbackInputStream.read();
//        int ch4 = pushbackInputStream.read();
//        if ((ch1 | ch2 | ch3 | ch4) < 0)
//            throw new EOFException();
//        if (!checkLastBytes()){
//            throw new IOException("Command id syntax error");
//        }
//        return new RespCommandId((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4));
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

    private boolean checkLastBytes() throws IOException{
        byte byte1 = (byte) pushbackInputStream.read();
        byte byte2 = (byte) pushbackInputStream.read();
        return (byte1 == CR) && (byte2 == LF);
    }

    private byte getNextByte() throws IOException {
        byte firstByte = (byte) pushbackInputStream.read();
        pushbackInputStream.unread(firstByte);
        return firstByte;
    }
}