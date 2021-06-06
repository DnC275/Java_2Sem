package com.itmo.java.basics;

import com.itmo.java.basics.console.*;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.InitializationContextImpl;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseServer {
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final ExecutionEnvironment environment;

    private DatabaseServer(ExecutionEnvironment environment){
        this.environment = environment;
    }

    /**
     * Конструктор
     *
     * @param env         env для инициализации. Далее работа происходит с заполненным объектом
     * @param initializer готовый чейн инициализации
     * @throws DatabaseException если произошла ошибка инициализации
     */
    public static DatabaseServer initialize(ExecutionEnvironment env, DatabaseServerInitializer initializer) throws DatabaseException {
        initializer.perform(new InitializationContextImpl(env));
        return new DatabaseServer(env);
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(RespArray message) {
        List<RespObject> objects = message.getObjects();
        RespObject commandIdObject = objects.get(DatabaseCommandArgPositions.COMMAND_ID.getPositionIndex());
        RespObject commandNameObject = objects.get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex());
        DatabaseCommand command = DatabaseCommands.valueOf(commandNameObject.asString()).getCommand(environment, objects);
        return executeNextCommand(command);
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(DatabaseCommand command) {
        return CompletableFuture.supplyAsync(command::execute, executorService);
    }

    public ExecutionEnvironment getEnv() {
        //TODO implement
        return null;
    }
}