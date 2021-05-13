package com.itmo.java.client.client;


import com.itmo.java.client.command.CreateDatabaseKvsCommand;
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
            CreateDatabaseKvsCommand createDatabaseKvsCommand = new CreateDatabaseKvsCommand(databaseName);
            RespObject object = kvsConnection.send(createDatabaseKvsCommand.getCommandId(), createDatabaseKvsCommand.serialize());
            return object.asString();
        }
        catch(ConnectionException e){
            throw new DatabaseExecutionException(e.getMessage(), e);
        }
    }

    @Override
    public String createTable(String tableName) throws DatabaseExecutionException {
        //TODO implement
        return null;
    }

    @Override
    public String get(String tableName, String key) throws DatabaseExecutionException {
        //TODO implement
        return null;
    }

    @Override
    public String set(String tableName, String key, String value) throws DatabaseExecutionException {
        //TODO implement
        return null;
    }

    @Override
    public String delete(String tableName, String key) throws DatabaseExecutionException {
        //TODO implement
        return null;
    }
}
