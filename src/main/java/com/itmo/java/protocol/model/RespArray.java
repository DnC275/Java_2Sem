package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Массив RESP объектов
 */
public class RespArray implements RespObject {
    private final List<RespObject> objects;

    /**
     * Код объекта
     */
    public static final byte CODE = '*';

    public RespArray(RespObject... objects) {
        this.objects = new LinkedList<>(Arrays.asList(objects));
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
     * @return результаты метода {@link RespObject#asString()} для всех хранимых объектов, разделенные пробелом
     */
    @Override
    public String asString() {
        StringBuilder builder = new StringBuilder();
        for (RespObject object:
             objects) {
            builder.append(object.asString());
            builder.append(" ");
        }
        builder.deleteCharAt(builder.length() - 1);
        return builder.toString();
    }

    @Override
    public void write(OutputStream os) throws IOException {
        //TODO implement
    }

    public List<RespObject> getObjects() {
        return new LinkedList<>(objects);
    }
}
