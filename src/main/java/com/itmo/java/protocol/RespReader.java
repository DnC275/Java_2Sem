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
//    private InputStream is;
    private PushbackInputStream is;
//    private DataInputStream dataInputStream;

    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    public RespReader(InputStream is) {
        this.is = new PushbackInputStream(is);
//        this.dataInputStream = new DatabaseInputStream(is);
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
//        byte b = (byte) is.read();
//        if (b != RespError.CODE)
//            throw new IOException(""); //TODO
        byte[] message = readToCRLF(is);
        return new RespError(message);
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
        byte b = (byte) is.read();
        if (b != RespBulkString.CODE)
            throw new IOException(""); //TODO
        byte[] skipStringLenght = readToCRLF(is);
        byte[] message = readToCRLF(is);
        return new RespBulkString(message);
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
        is.close();
    }

    private byte getNextByte() throws IOException {
        try {
//            PushbackInputStream pushbackInputStream = new PushbackInputStream(is);
            byte b = (byte) is.read();
            if (b == -1) {
                throw new EOFException(""); //TODO
            }
            is.unread(b);
            return b;
        }
        catch (EOFException e) {
            throw e;
        }
        catch (IOException e) {
            throw new IOException("", e);
        }
    }

    private byte[] readToCRLF(InputStream is) throws IOException {
        try {
            List<Byte> message = new ArrayList<>();
            byte b = (byte) is.read();
            while (true) {
                if (b == -1)
                    throw new EOFException(""); //TODO
                if (b != CR) {
                    message.add(b);
                    b = (byte) is.read();
                } else {
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
            throw new IOException("", e); //TODO
        }
    }
}
