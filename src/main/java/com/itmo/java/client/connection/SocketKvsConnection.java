package com.itmo.java.client.connection;

import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

import java.io.*;
import java.net.Socket;

/**
 * С помощью {@link RespWriter} и {@link RespReader} читает/пишет в сокет
 */
public class SocketKvsConnection implements KvsConnection {
    private final Socket clientSocket;
    private final ConnectionConfig connectionConfig;
    private final RespReader reader;
    private final RespWriter writer;

    public SocketKvsConnection(ConnectionConfig config) {
        this.connectionConfig = config;
        try {
            clientSocket = new Socket(connectionConfig.getHost(), connectionConfig.getPort());
            reader = new RespReader(clientSocket.getInputStream());
            writer = new RespWriter(clientSocket.getOutputStream());
        }
        catch (IOException e) {
            throw new RuntimeException("Error creating socket connection");
        }
    }

    /**
     * Отправляет с помощью сокета команду и получает результат.
     * @param commandId id команды (номер)
     * @param command   команда
     * @throws ConnectionException если сокет закрыт или если произошла другая ошибка соединения
     */
    @Override
    public synchronized RespObject send(int commandId, RespArray command) throws ConnectionException {
        RespObject object;
        try {
            writer.write(command);
        }
        catch (IOException e) {
            throw new ConnectionException("Error sending command");
        }
        try {
            object = reader.readObject();
        }
        catch (IOException e) {
            throw new ConnectionException(e.getMessage());
        }
        return object;
    }

    /**
     * Закрывает сокет (и другие использованные ресурсы)
     */
    @Override
    public void close() {
        try {
            reader.close();
            writer.close();
            clientSocket.close();
        }
        catch (IOException e) {
            throw new RuntimeException("Error closing socket connection");
        }
    }
}
