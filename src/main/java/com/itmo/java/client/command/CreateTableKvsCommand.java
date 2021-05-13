package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;

import java.nio.charset.StandardCharsets;

/**
 * Команда для создания таблицы
 */
public class CreateTableKvsCommand implements KvsCommand {
    private static final String COMMAND_NAME = "CREATE_TABLE";
    private final int commandId;
    private final String databaseName;
    private final String tableName;


    public CreateTableKvsCommand(String databaseName, String tableName) {
        this.commandId = idGen.getAndIncrement();
        this.databaseName = databaseName;
        this.tableName = tableName;
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
        return new RespArray(respCommandId, respBulkStringDb, respBulkStringTable);
    }

    @Override
    public int getCommandId() {
        return commandId;
    }
}
