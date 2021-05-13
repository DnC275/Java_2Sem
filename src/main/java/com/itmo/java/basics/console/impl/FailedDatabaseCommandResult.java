package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Зафейленная команда
 */
public class FailedDatabaseCommandResult implements DatabaseCommandResult {
    private final Optional<byte[]> payload;

    public FailedDatabaseCommandResult(String payload) {
        this.payload = Optional.of(payload.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Сообщение об ошибке
     */
    @Override
    public String getPayLoad() {
        return payload.map(String::new).orElse(null);
    }

    @Override
    public boolean isSuccess() {
        return false;
    }

    /**
     * Сериализуется в {@link RespError}
     */
    @Override
    public RespObject serialize() {
        return new RespError(getPayLoad().getBytes(StandardCharsets.UTF_8));
    }
}
