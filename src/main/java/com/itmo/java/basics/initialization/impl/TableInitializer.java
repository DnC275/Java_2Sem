package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.TableImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TableInitializer implements Initializer {
    SegmentInitializer segmentInit;

    public TableInitializer(SegmentInitializer segmentInitializer) {
        this.segmentInit = segmentInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой таблице.
     * Запускает инициализацию всех сегментов в порядке их создания (из имени)
     *
     * @param context контекст с информацией об инициализируемой бд, окружении, таблицы
     * @throws DatabaseException если в контексте лежит неправильный путь к таблице, невозможно прочитать содержимого папки,
     *  или если возникла ошибка ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        File path = Paths.get(context.currentTableContext().getTablePath().toString(), context.currentTableContext().getTableName()).toFile();
        if (!path.exists()){
            throw new DatabaseException(""); //TODO
        }
        else{
            for (File segment : path.listFiles()){
                InitializationContextImpl newInit = new InitializationContextImpl(context.executionEnvironment(), context.currentDbContext(), context.currentTableContext(), new SegmentInitializationContextImpl(segment.getName(), path.toPath(), (int)segment.length(), new SegmentIndex()));
                segmentInit.perform(newInit);
            }
            context.currentDbContext().addTable(TableImpl.initializeFromContext(context.currentTableContext()));
        }
    }
}
