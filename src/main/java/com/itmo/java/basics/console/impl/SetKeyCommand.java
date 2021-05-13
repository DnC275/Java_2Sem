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
 * Команда для создания записи значения
 */
public class SetKeyCommand implements DatabaseCommand {
    private final ExecutionEnvironment environment;
    private final List<RespObject> objects;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ, значение
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public SetKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        if (commandArgs.size() != 6){
            throw new IllegalArgumentException("Message"); //TODO
        }
        this.environment = env;
        this.objects = new LinkedList<>(commandArgs);
    }

    /**
     * Записывает значение
     *
     * @return {@link DatabaseCommandResult#success(byte[])} c предыдущим значением. Например, "previous" или null, если такого не было
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            String databaseName = objects.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
            String tableName = objects.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
            String key = objects.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();
            String value = objects.get(DatabaseCommandArgPositions.VALUE.getPositionIndex()).asString();
            Optional<Database> database = environment.getDatabase(databaseName);
            if (database.isEmpty()){
                throw new DatabaseException("Message"); //TODO
            }
            database.get().write(tableName, key, value.getBytes(StandardCharsets.UTF_8));
            return DatabaseCommandResult.success(String.format("Value of key '%s' was set successfully", key).getBytes(StandardCharsets.UTF_8));
        }
        catch(DatabaseException e){
            return new FailedDatabaseCommandResult(e.getMessage());
        }
    }
}
