package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.initialization.TableInitializationContext;

import java.nio.file.Path;

public class InitializationContextImpl implements InitializationContext {

    private final ExecutionEnvironment executionEnvironment;
    private final DatabaseInitializationContext currentDatabaseContext;
    private final TableInitializationContext currentTableContext;
    private final SegmentInitializationContext currentSegmentContext;

    public InitializationContextImpl(ExecutionEnvironment executionEnvironment,
                                      DatabaseInitializationContext currentDatabaseContext,
                                      TableInitializationContext currentTableContext,
                                      SegmentInitializationContext currentSegmentContext) {
        this.executionEnvironment = executionEnvironment;
        this.currentDatabaseContext = currentDatabaseContext;
        this.currentTableContext = currentTableContext;
        this.currentSegmentContext = currentSegmentContext;
    }
    public InitializationContextImpl(ExecutionEnvironment executionEnvironment){
        this.executionEnvironment = executionEnvironment;
        this.currentDatabaseContext = null;
        this.currentTableContext = null;
        this.currentSegmentContext = null;
    }

    @Override
    public ExecutionEnvironment executionEnvironment() {
        return executionEnvironment;
    }

    @Override
    public DatabaseInitializationContext currentDbContext() {
        return currentDatabaseContext;
    }

    @Override
    public TableInitializationContext currentTableContext() {
        return currentTableContext;
    }

    @Override
    public SegmentInitializationContext currentSegmentContext() {
        return currentSegmentContext;
    }
}
