package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Результат успешной команды
 */
public class SuccessDatabaseCommandResult implements DatabaseCommandResult {
    private final Optional<byte[]> payload;

    public SuccessDatabaseCommandResult(byte[] payload) {
        this.payload = Optional.of(payload);
    }

    @Override
    public String getPayLoad() {
        return payload.map(String::new).orElse(null);
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
        return new RespBulkString(getPayLoad().getBytes(StandardCharsets.UTF_8));
    }
}
