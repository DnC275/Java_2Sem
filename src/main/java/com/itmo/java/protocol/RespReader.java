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
    private Scanner scanner;
//    private DataInputStream dataInputStream;

    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    public RespReader(InputStream is) {
        this.scanner = new Scanner(is);
//        this.dataInputStream = new DatabaseInputStream(is);
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {
        String str = new String("*");
        return scanner.hasNext(str);
//        try {
//            byte check = dataInputStream.readByte();
//            return check == '*';
//        }
//        catch (EOFException e) {
//            return false;
//        }
    }

    /**
     * Считывает из input stream следующий объект. Может прочитать любой объект, сам определит его тип на основе кода объекта.
     * Например, если первый элемент "-", то вернет ошибку. Если "$" - bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespObject readObject() throws IOException {
        if (scanner.hasNext("-")) {
            return readError();
        }
        if (scanner.hasNext("$")) {
            return readBulkString();
        }
        if (scanner.hasNext("!")) {
            return readCommandId();
        }
        return null;
    }

    /**
     * Считывает объект ошибки
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespError readError() throws IOException {
        List<Byte> message = new ArrayList<>();
        byte b = scanner.nextByte();
        while (true) {
            if (b != CR) {
                message.add(b);
                b = scanner.nextByte();
            } else {
                b = scanner.nextByte();
                if (b == LF) {
                    break;
                }
                message.add(CR);
            }
        }
        byte[] result = new byte[message.size()];
        int i = 0;
        for (Byte character :
             message) {
            result[i++] = character;
        }
        return new RespError(result);

    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
        int size = scanner.nextInt();
        scanner.next();
        scanner.next();
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
}
