package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * Команда для создания удаления значения по ключу
 */
public class DeleteKeyCommand implements DatabaseCommand {
    private final ExecutionEnvironment environment;
    private final List<RespObject> objects;


    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public DeleteKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        if (commandArgs.size() != 5){
            throw new IllegalArgumentException("Message"); //TODO
        }
        this.environment = env;
        this.objects = new LinkedList<>(commandArgs);
    }

    /**
     * Удаляет значение по ключу
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с удаленным значением. Например, "previous"
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            String databaseName = objects.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
            String tableName = objects.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
            String key = objects.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();
            Optional<Database> database = environment.getDatabase(databaseName);
            if (database.isEmpty()){
                throw new DatabaseException("Message"); //TODO
            }
            database.get().delete(tableName, key);
            return new SuccessDatabaseCommandResult("Value deleted successfully".getBytes(StandardCharsets.UTF_8));
        }
        catch(DatabaseException e){
            return new FailedDatabaseCommandResult(e.getMessage());
        }
    }
}
