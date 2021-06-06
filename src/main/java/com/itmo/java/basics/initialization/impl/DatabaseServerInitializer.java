package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import java.io.File;

public class DatabaseServerInitializer implements Initializer {
    DatabaseInitializer dbInit;

    public DatabaseServerInitializer(DatabaseInitializer databaseInitializer) {
        this.dbInit = databaseInitializer;
    }

    /**
     * Если заданная в окружении директория не существует - создает ее
     * Добавляет информацию о существующих в директории базах, начинает их инициализацию
     *
     * @param context контекст, содержащий информацию об окружении
     * @throws DatabaseException если произошла ошибка при создании директории, ее обходе или ошибка инициализации бд
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        File path = context.executionEnvironment().getWorkingPath().toFile();
        if (!path.exists()){
            if (!path.mkdirs()) {
                throw new DatabaseException(String.format("Cannot make default dir in \"%s\"", path));
            }
        }
        else{
            for (File db : path.listFiles()){
                InitializationContextImpl newInit = new InitializationContextImpl(context.executionEnvironment(), new DatabaseInitializationContextImpl(db.getName(), path.toPath()), context.currentTableContext(), context.currentSegmentContext());
                dbInit.perform(newInit);
            }
        }
    }
}
