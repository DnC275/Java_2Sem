package com.itmo.java.client.client;


import com.itmo.java.client.command.*;
import com.itmo.java.client.connection.KvsConnection;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.client.exception.DatabaseExecutionException;
import com.itmo.java.protocol.model.RespObject;

import java.util.function.Supplier;

public class SimpleKvsClient implements KvsClient {
    private final String databaseName;
    private final KvsConnection kvsConnection;

    /**
     * Конструктор
     *
     * @param databaseName       имя базы, с которой работает
     * @param connectionSupplier метод создания подключения к базе
     */
    public SimpleKvsClient(String databaseName, Supplier<KvsConnection> connectionSupplier) {
        this.databaseName = databaseName;
        this.kvsConnection = connectionSupplier.get();
    }

    @Override
    public String createDatabase() throws DatabaseExecutionException {
        KvsCommand createDatabaseKvsCommand = new CreateDatabaseKvsCommand(databaseName);
        return sendCommand(createDatabaseKvsCommand);
    }

    @Override
    public String createTable(String tableName) throws DatabaseExecutionException {
        KvsCommand createTableKvsCommand = new CreateTableKvsCommand(databaseName, tableName);
        return sendCommand(createTableKvsCommand);
    }

    @Override
    public String get(String tableName, String key) throws DatabaseExecutionException {
        KvsCommand getKvsCommand = new GetKvsCommand(databaseName, tableName, key);
        return sendCommand(getKvsCommand);
    }

    @Override
    public String set(String tableName, String key, String value) throws DatabaseExecutionException {
        KvsCommand setKvsCommand = new SetKvsCommand(databaseName, tableName, key, value);
        return sendCommand(setKvsCommand);
    }

    @Override
    public String delete(String tableName, String key) throws DatabaseExecutionException {
        KvsCommand deleteKvsCommand = new DeleteKvsCommand(databaseName, tableName, key);
        return sendCommand(deleteKvsCommand);
    }

    private String sendCommand(KvsCommand command) throws DatabaseExecutionException {
        try {
            RespObject object = kvsConnection.send(command.getCommandId(), command.serialize());
            if (object.isError()){
                throw new DatabaseExecutionException(object.asString());
            }
            return object.asString();
        }
        catch(ConnectionException e) {
            throw new DatabaseExecutionException(e.getMessage(), e);
        }
    }
}
