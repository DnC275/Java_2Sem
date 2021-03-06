package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Segment;

import java.nio.file.Path;
import java.nio.file.Paths;

public class TableInitializationContextImpl implements TableInitializationContext {
    private final String tableName;
    private final Path tablePath;
    private final TableIndex tableIndex;
    private Segment currentSegment;

    public TableInitializationContextImpl(String tableName, Path databasePath, TableIndex tableIndex) {
        this.tableName = tableName;
        this.tablePath = databasePath;
        this.tableIndex = tableIndex;
        this.currentSegment = null;
    }

    @Override
    public String getTableName() {
        return this.tableName;
    }

    @Override
    public Path getTablePath() {
        return Paths.get(tablePath.toString(), tableName);
    }

    @Override
    public TableIndex getTableIndex() {
        return this.tableIndex;
    }

    @Override
    public Segment getCurrentSegment() {
        return this.currentSegment;
    }

    @Override
    public void updateCurrentSegment(Segment segment) {
        currentSegment = segment;
    }
}
