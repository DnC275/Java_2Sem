package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;

import java.nio.charset.StandardCharsets;

public class SetKvsCommand implements KvsCommand {
    private static final String COMMAND_NAME = "SET_KEY";
    private final int commandId;
    private final String databaseName;
    private final String tableName;
    private final String key;


    public SetKvsCommand(String databaseName, String tableName, String key, String value) {
        this.commandId = idGen.getAndIncrement();
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.key = key;
    }

    /**
     * Возвращает RESP объект. {@link RespArray} с {@link RespCommandId}, именем команды, аргументами в виде {@link RespBulkString}
     *
     * @return объект
     */
    @Override
    public RespArray serialize() {
        RespCommandId respCommandId = new RespCommandId(this.getCommandId());
        RespBulkString respBulkStringDb = new RespBulkString(databaseName.getBytes(StandardCharsets.UTF_8));
        RespBulkString respBulkStringTable = new RespBulkString(tableName.getBytes(StandardCharsets.UTF_8));
        RespBulkString respBulkStringKey = new RespBulkString(key.getBytes(StandardCharsets.UTF_8));
        return new RespArray(respCommandId, respBulkStringDb, respBulkStringTable, respBulkStringKey);
    }

    @Override
    public int getCommandId() {
        return commandId;
    }
}
