package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;

import java.nio.charset.StandardCharsets;

/**
 * Команда для создания бд
 */
public class CreateDatabaseKvsCommand implements KvsCommand {
    private static final String COMMAND_NAME = "CREATE_DATABASE";
    private final int commandId;
    private final String databaseName;

    /**
     * Создает объект
     *
     * @param databaseName имя базы данных
     */
    public CreateDatabaseKvsCommand(String databaseName) {
        this.commandId = idGen.getAndIncrement();
        this.databaseName = databaseName;
    }

    /**
     * Возвращает RESP объект. {@link RespArray} с {@link RespCommandId}, именем команды, аргументами в виде {@link RespBulkString}
     *
     * @return объект
     */
    @Override
    public RespArray serialize() {
        //Change naming
        RespCommandId respCommandId = new RespCommandId(this.getCommandId());
        RespBulkString respCommandName = new RespBulkString(COMMAND_NAME.getBytes(StandardCharsets.UTF_8));
        RespBulkString respBulkString = new RespBulkString(databaseName.getBytes(StandardCharsets.UTF_8));
        return new RespArray(respCommandId, respCommandName, respBulkString);
    }

    @Override
    public int getCommandId() {
        return commandId;
    }
}
