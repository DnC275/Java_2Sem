package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.File;

public class DatabaseInitializer implements Initializer {
    private TableInitializer tableInit;

    public DatabaseInitializer(TableInitializer tableInitializer) {
        this.tableInit = tableInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой бд.
     * Запускает инициализацию всех таблиц это базы
     *
     * @param context контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к базе, невозможно прочитать содержимого папки,
     *  или если возникла ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        File path = context.executionEnvironment().getWorkingPath().toFile();
        if (!path.exists()){
            throw new DatabaseException(""); //TODO
        }
        else{
            for (File table : path.listFiles()){
                InitializationContextImpl newInit = new InitializationContextImpl(context.executionEnvironment(), context.currentDbContext(), new TableInitializationContextImpl(table.getName(), path.toPath(), new TableIndex()), context.currentSegmentContext());
                tableInit.perform(newInit);
            }
            context.executionEnvironment().addDatabase(DatabaseImpl.initializeFromContext(context.currentDbContext()));
        }
    }
}
