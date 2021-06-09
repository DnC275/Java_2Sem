package com.itmo.java.client.connection;

import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * С помощью {@link RespWriter} и {@link RespReader} читает/пишет в сокет
 */
public class SocketKvsConnection implements KvsConnection {
    Socket clientSocket;
    ConnectionConfig connectionConfig;
//    DataInputStream is;
//    DataOutputStream os;
    InputStream is;
    OutputStream os;

    public SocketKvsConnection(ConnectionConfig config) {
        this.connectionConfig = config;
        try {
            clientSocket = new Socket(ConnectionConfig.DEFAULT_HOST, ConnectionConfig.DEFAULT_PORT);
//            is = new DataInputStream(clientSocket.getInputStream());
//            os = new DataOutputStream(clientSocket.getOutputStream());
            is = clientSocket.getInputStream();
            os = clientSocket.getOutputStream();
        }
        catch (IOException e) {
            throw new RuntimeException("Errors with socket kvs connection");
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
        try {
            command.write(os);
            RespReader respReader = new RespReader(is);
            RespArray respArray = respReader.readArray();
            return respArray;
        }
        catch (IOException e) {
            throw new ConnectionException("Something wrong with connection", e);
        }
    }

    /**
     * Закрывает сокет (и другие использованные ресурсы)
     */
    @Override
    public void close() {
        try {
            is.close();
            os.close();
        }
        catch (IOException e) {
            throw new RuntimeException("Error while closing connection");
        }
    }
}
