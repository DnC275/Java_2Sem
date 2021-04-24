package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.TableImpl;


import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;

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
     *                           или если возникла ошибка ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        File path = context.currentTableContext().getTablePath().toFile();
        if (!path.exists()) {
            throw new DatabaseException(""); //TODO
        } else {
            File[] a = path.listFiles();
            Arrays.sort(a);
            for (File segment : a) {
                InitializationContextImpl newInit = new InitializationContextImpl(context.executionEnvironment(), context.currentDbContext(), context.currentTableContext(),
                        new SegmentInitializationContextImpl(segment.getName(), Paths.get(path.toPath().toString(), segment.getName()), 0, new SegmentIndex()));
                segmentInit.perform(newInit);
            }
            context.currentDbContext().addTable(TableImpl.initializeFromContext(context.currentTableContext()));
        }
    }
}
