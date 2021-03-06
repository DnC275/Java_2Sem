package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.DatabaseFactory;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

/**
 * Команда для создания базы данных
 */
public class CreateDatabaseCommand implements DatabaseCommand {
    private static final int OBJECTS_COUNT = 3;
    private final ExecutionEnvironment environment;
    private final DatabaseFactory factory;
    private final List<RespObject> objects;


    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param factory     функция создания базы данных (пример: DatabaseImpl::create)
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя создаваемой бд
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public CreateDatabaseCommand(ExecutionEnvironment env, DatabaseFactory factory, List<RespObject> commandArgs) {
        if (commandArgs.size() != OBJECTS_COUNT) {
            throw new IllegalArgumentException(String.format("Incorrect number of arguments. expected: '%d', but was: %d", OBJECTS_COUNT, commandArgs.size()));
        }
        this.environment = env;
        this.factory = factory;
        this.objects = new LinkedList<>(commandArgs);
    }

    /**
     * Создает бд в нужном env
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с сообщением о том, что заданная база была создана. Например, "Database db1 created"
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            String databaseName = objects.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
            Database database = factory.createNonExistent(databaseName, environment.getWorkingPath());
            environment.addDatabase(database);
            return DatabaseCommandResult.success(String.
                    format("Database '%s' was created successfully", databaseName).getBytes(StandardCharsets.UTF_8));
        }
        catch(DatabaseException e){
            return DatabaseCommandResult.error(e);
        }
    }
}
