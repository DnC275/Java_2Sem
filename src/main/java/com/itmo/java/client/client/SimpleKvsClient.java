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
     * Констурктор
     *
     * @param databaseName       имя базы, с которой работает
     * @param connectionSupplier метод создания коннекшена к базе
     */
    public SimpleKvsClient(String databaseName, Supplier<KvsConnection> connectionSupplier) {
        this.databaseName = databaseName;
        this.kvsConnection = connectionSupplier.get();
    }

    @Override
    public String createDatabase() throws DatabaseExecutionException {
        try {
            KvsCommand createDatabaseKvsCommand = new CreateDatabaseKvsCommand(databaseName);
            RespObject object = kvsConnection.send(createDatabaseKvsCommand.getCommandId(), createDatabaseKvsCommand.serialize());
            return object.asString();
        }
        catch(ConnectionException e){
            throw new DatabaseExecutionException(e.getMessage(), e);
        }
    }

    @Override
    public String createTable(String tableName) throws DatabaseExecutionException {
        try {
            KvsCommand createTableKvsCommand = new CreateTableKvsCommand(databaseName, tableName);
            RespObject object = kvsConnection.send(createTableKvsCommand.getCommandId(), createTableKvsCommand.serialize());
            return object.asString();
        }
        catch(ConnectionException e){
            throw new DatabaseExecutionException(e.getMessage(), e);
        }
    }

    @Override
    public String get(String tableName, String key) throws DatabaseExecutionException {
        try {
            KvsCommand getKvsCommand = new GetKvsCommand(databaseName, tableName, key);
            RespObject object = kvsConnection.send(getKvsCommand.getCommandId(), getKvsCommand.serialize());
            return object.asString();
        }
        catch(ConnectionException e){
            throw new DatabaseExecutionException(e.getMessage(), e);
        }
    }

    @Override
    public String set(String tableName, String key, String value) throws DatabaseExecutionException {
        try {
            KvsCommand setKvsCommand = new SetKvsCommand(databaseName, tableName, key, value);
            RespObject object = kvsConnection.send(setKvsCommand.getCommandId(), setKvsCommand.serialize());
            return object.asString();
        }
        catch(ConnectionException e){
            throw new DatabaseExecutionException(e.getMessage(), e);
        }
    }

    @Override
    public String delete(String tableName, String key) throws DatabaseExecutionException {
        try {
            KvsCommand deleteKvsCommand = new DeleteKvsCommand(databaseName, tableName, key);
            RespObject object = kvsConnection.send(deleteKvsCommand.getCommandId(), deleteKvsCommand.serialize());
            return object.asString();
        }
        catch(ConnectionException e){
            throw new DatabaseExecutionException(e.getMessage(), e);
        }
    }
}
