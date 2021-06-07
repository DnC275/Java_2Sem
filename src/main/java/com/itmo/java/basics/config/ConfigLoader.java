package com.itmo.java.basics.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.Properties;

/**
 * Класс, отвечающий за подгрузку данных из конфигурационного файла формата .properties
 */
public class ConfigLoader {
    String configFileName;

    /**
     * По умолчанию читает из server.properties
     */
    public ConfigLoader() {
        configFileName = "server.properties";
    }

    /**
     * @param name Имя конфикурационного файла, откуда читать
     */
    public ConfigLoader(String name) {
        configFileName = name;
    }

    /**
     * Считывает конфиг из указанного в конструкторе файла.
     * Если не удалось считать из заданного файла, или какого-то конкретно значения не оказалось,
     * то используют дефолтные значения из {@link DatabaseConfig} и {@link ServerConfig}
     * <br/>
     * Читаются: "kvs.workingPath", "kvs.host", "kvs.port" (но в конфигурационном файле допустимы и другие проперти)
     */
    public DatabaseServerConfig readConfig() {
        Properties defaults = getDefaultProperties();
        DatabaseServerConfig.DatabaseServerConfigBuilder databaseServerConfigBuilder = new DatabaseServerConfig.DatabaseServerConfigBuilder();
        Properties properties = new Properties(defaults);
        try (InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(configFileName)){
            properties.load(inputStream);
        }
        catch (IOException | NullPointerException e) {
            properties = defaults;
        }
        databaseServerConfigBuilder.dbConfig(new DatabaseConfig(properties.getProperty("kvs.workingPath")));
        databaseServerConfigBuilder.serverConfig(new ServerConfig(properties.getProperty("kvs.host"), Integer.parseInt(properties.getProperty("kvs.port"))));
        return databaseServerConfigBuilder.build();
    }

    private Properties getDefaultProperties() {
        Properties defaults = new Properties();
        defaults.setProperty("kvs.workingPath", DatabaseConfig.DEFAULT_WORKING_PATH);
        defaults.setProperty("kvs.host", ServerConfig.DEFAULT_HOST);
        defaults.setProperty("kvs.port", Integer.toString(ServerConfig.DEFAULT_PORT));
        return defaults;
    }
}
