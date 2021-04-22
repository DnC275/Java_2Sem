package com.itmo.java.basics.logic.io;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

class Main {
    public static void main(String[] args) throws DatabaseException {
//        Database db = DatabaseImpl.create("DatabaseTest", Paths.get("/home/denis/TechProg/Lab2"));
//        db.createTableIfNotExists("123");
//        byte[] value = {1, 2, 3};
//        db.write("123", "12", null);
//        Optional<byte[]> checkValue = db.read("123", "12");
//        if (checkValue.isEmpty()){
//            System.out.println("!!!");
//        }
//        db.createTableIfNotExists("456");
//        db.write("456", "1", "5".getBytes(StandardCharsets.UTF_8));
//        byte[] v = {1, 1, 1, 1};
//        db.write("123", "1", v);
//        Optional<byte[]> b = db.read("123","1");
//        for (byte i:
//             b.get()) {
//            System.out.println(i);
//        }
//        db.delete("123", "1");
//        Optional<byte[]> c = db.read("167","1");

    }
}