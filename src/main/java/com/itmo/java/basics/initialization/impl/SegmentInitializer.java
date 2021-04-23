package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.initialization.impl.SegmentInitializationContextImpl.SegmentInitializationContextImplBuilder;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;


public class SegmentInitializer implements Initializer {

    /**
     * Добавляет в контекст информацию об инициализируемом сегменте.
     * Составляет индекс сегмента
     * Обновляет инфу в индексе таблицы
     *
     * @param context контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к сегменту, невозможно прочитать содержимое. Ошибка в содержании
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        Path path = context.currentSegmentContext().getSegmentPath();
        String name = context.currentSegmentContext().getSegmentName();
        System.out.println(path);
        System.out.println(name);
        try (DatabaseInputStream inputStream = new DatabaseInputStream(new FileInputStream(path.toString()))){
            Optional<DatabaseRecord> record = inputStream.readDbUnit();
            long currentSize = 0;
            while (record.isPresent()){
                String key = new String(record.get().getKey(), StandardCharsets.UTF_8);
                context.currentSegmentContext().getIndex().onIndexedEntityUpdated(key, new SegmentOffsetInfoImpl(currentSize));
                currentSize += record.get().size();
                record = inputStream.readDbUnit();
            }
            context.currentTableContext().updateCurrentSegment(SegmentImpl.initializeFromContext(context.currentSegmentContext()));
        }
        catch (FileNotFoundException ex){
            throw new DatabaseException(""); //TODO
        }
        catch (IOException ex){
            throw new DatabaseException(""); //TODO
        }
    }
}
