package com.itmo.java.basics.connector;

import com.itmo.java.basics.DatabaseServer;
import com.itmo.java.basics.config.ConfigLoader;
import com.itmo.java.basics.config.ServerConfig;
import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.resp.CommandReader;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespObject;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Класс, который предоставляет доступ к серверу через сокеты
 */
public class JavaSocketServerConnector implements Closeable {

    /**
     * Экзекьютор для выполнения ClientTask
     */
    private final ExecutorService clientIOWorkers = Executors.newSingleThreadExecutor();
    private final ExecutorService connectionAcceptorExecutor = Executors.newSingleThreadExecutor();
    private final ServerSocket serverSocket;
    private final DatabaseServer server;

    /**
     * Стартует сервер. По аналогии с сокетом открывает коннекшн в конструкторе.
     */
    public JavaSocketServerConnector(DatabaseServer databaseServer, ServerConfig config) throws IOException {
        serverSocket = new ServerSocket(config.getPort());
        this.server = databaseServer;
    }
 
     /**
     * Начинает слушать заданный порт, начинает аксептить клиентские сокеты. На каждый из них начинает клиентскую таску
     */
    public void start() {
        connectionAcceptorExecutor.submit(() -> {
            while (true) {
                try {
                    if (serverSocket.isClosed())
                        break;
                    Socket clientSocket = serverSocket.accept();
                    clientIOWorkers.submit(new ClientTask(clientSocket, server));
                }
                catch (IOException e) {
                    close();
                    throw new RuntimeException("", e); //TODO
                }
            }
        });
    }

    /**
     * Закрывает все, что нужно ¯\_(ツ)_/¯
     */
    @Override
    public void close() {
//        System.out.println("Stopping socket connector");
//        try {
//            connectionAcceptorExecutor.shutdown();
//            serverSocket.close();
//        }
//        catch (IOException e) {
//            throw new RuntimeException("", e); //TODO
//        }
    }


    public static void main(String[] args) throws Exception {
        //TODO
    }

    /**
     * Runnable, описывающий исполнение клиентской команды.
     */
    static class ClientTask implements Runnable, Closeable {
        private final Socket client;
        private final DatabaseServer server;
        private final InputStream is;
        private final OutputStream os;

        /**
         * @param client клиентский сокет
         * @param server сервер, на котором исполняется задача
         */
        public ClientTask(Socket client, DatabaseServer server) {
            try {
                this.client = client;
                this.server = server;
                is = client.getInputStream();
                os = client.getOutputStream();
            }
            catch (IOException e) {
                throw new RuntimeException("", e);
            }
        }

        /**
         * Исполняет задачи из одного клиентского сокета, пока клиент не отсоединился или текущий поток не был прерван (interrupted).
         * Для кажной из задач:
         * 1. Читает из сокета команду с помощью {@link CommandReader}
         * 2. Исполняет ее на сервере
         * 3. Записывает результат в сокет с помощью {@link RespWriter}
         */
        @Override
        public void run() {
            try {
                CommandReader commandReader = new CommandReader(new RespReader(is), server.getEnv());
                DatabaseCommand command = commandReader.readCommand();
                DatabaseCommandResult result = server.executeNextCommand(command).get();
                RespWriter writer = new RespWriter(os);
                RespObject object = result.serialize();
                writer.write(object);
            }
            catch (IOException | InterruptedException | ExecutionException e) {
                throw new RuntimeException("", e); //TODO
            }
        }

        /**
         * Закрывает клиентский сокет
         */
        @Override
        public void close() {
            try {
                client.close();
            }
            catch (IOException e) {
                throw new RuntimeException("", e); //TODO
            }
        }
    }
}
