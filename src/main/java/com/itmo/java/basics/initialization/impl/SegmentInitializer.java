package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;


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
        Set<String> s = new HashSet<>();
        try (DatabaseInputStream inputStream = new DatabaseInputStream(new FileInputStream(path.toString()))){
//            Segment segment = SegmentImpl.initializeFromContext(context.currentSegmentContext());
            Optional<DatabaseRecord> record = inputStream.readDbUnit();
            long currentSize = 0;
            while (record.isPresent()){
                String key = new String(record.get().getKey(), StandardCharsets.UTF_8);
                context.currentSegmentContext().getIndex().onIndexedEntityUpdated(key, new SegmentOffsetInfoImpl(currentSize));
                s.add(key);
//                context.currentTableContext().getTableIndex().onIndexedEntityUpdated(key, segment);
                currentSize += record.get().size();
                record = inputStream.readDbUnit();
            }
            SegmentInitializationContextImpl newInit = new SegmentInitializationContextImpl(context.currentSegmentContext().getSegmentName(), path, (int)currentSize, context.currentSegmentContext().getIndex());
            Segment segment = SegmentImpl.initializeFromContext(newInit);
            for (String key : s) {
                context.currentTableContext().getTableIndex().onIndexedEntityUpdated(key, segment);
            }
            context.currentTableContext().updateCurrentSegment(segment);
        }
        catch (FileNotFoundException ex){
            throw new DatabaseException(String.format("File \"%s\" not found", path));
        }
        catch (IOException ex){
            throw new DatabaseException(String.format("IO actions with \"%s\" failed", path));
        }
    }
}
