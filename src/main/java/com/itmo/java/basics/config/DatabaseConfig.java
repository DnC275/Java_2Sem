package com.itmo.java.basics.config;

public class DatabaseConfig {
    public static final String DEFAULT_WORKING_PATH = "db_files";
    private String workindPath;

    public DatabaseConfig(){
        workindPath = DEFAULT_WORKING_PATH;
    }

    public DatabaseConfig(String workingPath) {
        this.workindPath = workingPath;
    }

    public String getWorkingPath() {
        return workindPath;
    }
}
