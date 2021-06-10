package com.itmo.java.basics.resp;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommands;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.util.List;

public class CommandReader implements AutoCloseable {
    RespReader reader;
    ExecutionEnvironment env;

    public CommandReader(RespReader reader, ExecutionEnvironment env) {
        this.reader = reader;
        this.env = env;
    }

    /**
     * Есть ли следующая команда в ридере?
     */
    public boolean hasNextCommand() throws IOException {
        return reader.hasArray();
    }

    /**
     * Считывает комманду с помощью ридера и возвращает ее
     *
     * @throws IllegalArgumentException если нет имени команды и id
     */
    public DatabaseCommand readCommand() throws IOException {
        List<RespObject> objects = reader.readArray().getObjects();
        System.out.println("I sosu");
        if (!checkObjectsCorrectness(objects)) {
            throw new IllegalArgumentException("Illegal argument in respReader readCommand"); //TODO
        }
        System.out.println("I vse eshe sosu");
        RespObject commandNameObject = objects.get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex());
        System.out.println("I uze ne sosijihlk");
        return DatabaseCommands.valueOf(commandNameObject.asString()).getCommand(env, objects);
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }

    private boolean checkObjectsCorrectness(List<RespObject> objects) {
        return objects.size() >= 3 && objects.get(DatabaseCommandArgPositions.COMMAND_ID.getPositionIndex()).getClass() == RespCommandId.class &&
                objects.get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex()).getClass() == RespBulkString.class;
    }
}
