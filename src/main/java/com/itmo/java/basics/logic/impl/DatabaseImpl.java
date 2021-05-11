package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DatabaseImpl implements Database {
    private String name;
    private Path path;
    private Map<String, Table> tableMap;

    private DatabaseImpl(String name, Path path){
        this.name = name;
        this.path = path;
        this.tableMap = new HashMap<>();
    }

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        if (!(new File(databaseRoot.toString())).exists()) {
            throw new DatabaseException(String.format("Failed to create a database by path \"%s\"", databaseRoot));
        }
        Path fullPath = FileSystems.getDefault().getPath(databaseRoot.toString(), dbName);
        File file = new File(fullPath.toString());
        if (!file.mkdir()) {
            throw new DatabaseException(String.format("Failed to create a database by path \"%s\"", databaseRoot));
        }
        return new DatabaseImpl(dbName, fullPath);
    }


    public static Database initializeFromContext(DatabaseInitializationContext context) {
        DatabaseImpl db = new DatabaseImpl(context.getDbName(), context.getDatabasePath());
        db.tableMap = context.getTables();
        return db;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (tableMap.containsKey(tableName)){
            throw new DatabaseException(String.format("Table with name \"%s\" already exists", tableName));
        }
        tableMap.put(tableName, TableImpl.create(tableName, path, new TableIndex()));
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        if (checkTableExistence(tableName)) {
            tableMap.get(tableName).write(objectKey, objectValue);
        }
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        if (checkTableExistence(tableName)) {
            return tableMap.get(tableName).read(objectKey);
        }
        return Optional.empty();
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        if (checkTableExistence(tableName)){
            tableMap.get(tableName).delete(objectKey);
        }
    }

    private boolean checkTableExistence(String tableName) throws DatabaseException {
        if (!tableMap.containsKey(tableName)){
            throw new DatabaseException(String.format("There is no table with name \"%s\"", tableName));
        }
        return true;
    }
}
