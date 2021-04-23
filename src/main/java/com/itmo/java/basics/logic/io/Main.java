package com.itmo.java.basics.logic.io;

import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.console.impl.ExecutionEnvironmentImpl;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.initialization.impl.*;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

class Main {
    public static void main(String[] args) throws DatabaseException {
        Database db = DatabaseImpl.create("Database2", Paths.get("/home/denis/TechProg/Lab2/Databases"));
        db.createTableIfNotExists("123");
        byte[] value = {1, 2, 3};
        db.write("123", "12", value);
        db.createTableIfNotExists("456");
        db.write("456", "1", "5".getBytes(StandardCharsets.UTF_8));
        byte[] v = {1, 1, 1, 1};
        db.write("123", "1", v);
        System.out.println(new String(db.read("123", "12").get(), StandardCharsets.UTF_8));

        // [B@30c7da1e

//        ExecutionEnvironment ex = new ExecutionEnvironmentImpl(new DatabaseConfig("Databases"));
//        Initializer init = new DatabaseServerInitializer(new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));
//        InitializationContext context = new InitializationContextImpl(ex, new DatabaseInitializationContextImpl("", Path.of("")),
//                new TableInitializationContextImpl("", Path.of(""), new TableIndex()), new SegmentInitializationContextImpl("", Path.of(""), 0, new SegmentIndex()));
//        init.perform(context);
//        Optional<Database> db = ex.getDatabase("Database2");
//        Database d = db.get();
//        Optional<byte[]> a = d.read("123", "12");
//        System.out.println(db.get().read("123", "12").get());
    }
}