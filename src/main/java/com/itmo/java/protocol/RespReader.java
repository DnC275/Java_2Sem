package com.itmo.java.protocol;

import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class RespReader implements AutoCloseable {
    private InputStream is;
//    private DataInputStream dataInputStream;

    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    public RespReader(InputStream is) {
        this.is = is;
//        this.dataInputStream = new DatabaseInputStream(is);
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {
        return getFirstByte() == RespArray.CODE;
    }

    /**
     * Считывает из input stream следующий объект. Может прочитать любой объект, сам определит его тип на основе кода объекта.
     * Например, если первый элемент "-", то вернет ошибку. Если "$" - bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespObject readObject() throws IOException {
        byte firstByte = getFirstByte();
        switch (firstByte) {
            case RespCommandId.CODE:
                return readCommandId();
            case RespBulkString.CODE:
                return readBulkString();
            case RespArray.CODE:
                return readArray();
            case RespError.CODE:
                return readError();
            default:
                throw new IOException(""); //TODO
        }
    }

    /**
     * Считывает объект ошибки
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespError readError() throws IOException {
        List<Byte> message = new ArrayList<>();
        byte b = (byte) is.read();
        while (true) {
            if (b != CR) {
                message.add(b);
                b = (byte) is.read();
            }
            else {
                b = (byte) is.read();
                if (b == LF) {
                    message.add(CR);
                    message.add(LF);
                    break;
                }
                message.add(CR);
            }
        }
        byte[] result = new byte[message.size()];
        int i = 0;
        for (Byte character : message)
            result[i++] = character;
        return new RespError(result);
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
        return null;
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
        //TODO implement
        return null;
    }

    /**
     * Считывает id команды
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespCommandId readCommandId() throws IOException {
        //TODO implement
        return null;
    }


    @Override
    public void close() throws IOException {
        //TODO implement
    }

    private byte getFirstByte() throws IOException {
        PushbackInputStream pushbackInputStream = new PushbackInputStream(is);
        byte firstByte = (byte) pushbackInputStream.read();
        if (firstByte != -1) {
            throw new EOFException(""); //TODO
        }
        pushbackInputStream.unread(1);
        return firstByte;
    }
}
