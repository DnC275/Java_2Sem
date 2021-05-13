package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;

/**
 * Результат успешной команды
 */
public class SuccessDatabaseCommandResult implements DatabaseCommandResult {
    private final String payload;

    public SuccessDatabaseCommandResult(byte[] payload) {
        this.payload = new String(payload, StandardCharsets.UTF_8);
    }

    @Override
    public String getPayLoad() {
        return payload;
    }

    @Override
    public boolean isSuccess() {
        return true;
    }

    /**
     * Сериализуется в {@link RespBulkString}
     */
    @Override
    public RespObject serialize() {
        //TODO implement
        return null;
    }
}
