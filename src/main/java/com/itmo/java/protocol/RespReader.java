package com.itmo.java.protocol;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;

import java.io.*;
import java.lang.reflect.Array;
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

    private final PushbackInputStream stream;

    public RespReader(InputStream is) {
        stream = new PushbackInputStream(is);
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {
        return returnFirstByte() == RespArray.CODE;
    }

    private byte returnFirstByte() throws IOException {
        byte firstByte = (byte)stream.read();
        stream.unread(firstByte);
        return firstByte;
    }

    /**
     * Считывает из input stream следующий объект. Может прочитать любой объект, сам определит его тип на основе кода объекта.
     * Например, если первый элемент "-", то вернет ошибку. Если "$" - bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespObject readObject() throws IOException {
        switch (returnFirstByte()){
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
                throw new IOException("Unknown Object");
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
        byte type = (byte) stream.read();
        if (type != RespError.CODE)
            throw new IOException("Error syntax error");
        return new RespError(readBeforeSeparator());
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
        byte type = (byte) stream.read();
        if (type != RespBulkString.CODE)
            throw new IOException("Bulk String syntax error");
        int size = Integer.parseInt(new String(readBeforeSeparator()));
        if (size == -1){
            return RespBulkString.NULL_STRING;
        }
        byte[] data = stream.readNBytes(size);
        if (!checkLastBytes()){
            throw new IOException("Bulk String syntax error");
        }
        return new RespBulkString(data);
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
        byte type = (byte) stream.read();
        if (type != RespArray.CODE)
            throw new IOException("Array syntax error");
        int size = Integer.parseInt(new String(readBeforeSeparator(), StandardCharsets.UTF_8));
        if (size < 1) {
            throw new IOException("");
        }
        RespObject[] objectList = new RespObject[size];
        for (int i = 0; i < size; i++){
            objectList[i] = readObject();
        }
        return new RespArray(objectList);
    }

    /**
     * Считывает id команды
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespCommandId readCommandId() throws IOException {
        byte type = (byte) stream.read();
        if (type != RespCommandId.CODE)
            throw new IOException("Command id syntax error");
        int ch1 = stream.read();
        int ch2 = stream.read();
        int ch3 = stream.read();
        int ch4 = stream.read();
        if ((ch1 | ch2 | ch3 | ch4) < 0)
            throw new EOFException();
        if (!checkLastBytes()){
            throw new IOException("Command id syntax error");
        }
        return new RespCommandId((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4));
    }


    @Override
    public void close() throws IOException {
        stream.close();
    }

    private byte[] readBeforeSeparator() throws IOException{
        ArrayList<Byte> bytes = new ArrayList<>();
        while (true){
            byte currentByte = (byte)stream.read();
            bytes.add(currentByte);
            while (currentByte == CR) {
                byte nextByte = (byte)stream.read();
                if (nextByte == LF) {
                    byte[] byteArray = new byte[bytes.size()-1];
                    for (int i = 0; i < bytes.size() - 1; i++) {
                        byteArray[i] = bytes.get(i);
                    }
                    return byteArray;
                }
                currentByte = nextByte;
                bytes.add(currentByte);
            }
        }
    }

    private boolean checkLastBytes() throws IOException{
        byte byte1 = (byte) stream.read();
        byte byte2 = (byte) stream.read();
        return (byte1 == CR) && (byte2 == LF);
    }
}