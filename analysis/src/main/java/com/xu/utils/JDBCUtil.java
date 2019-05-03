package com.xu.utils;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

public class JDBCUtil {
    private static Properties properties = null;
    private static Connection connection = null;

    static {
        InputStream resourceAsStream = ClassLoader.getSystemResourceAsStream("jdbc.properties");
        properties = new Properties();
        try {
            properties.load(resourceAsStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Connection getConnection() {
        try {
            Class.forName(properties.getProperty("MYSQL_DRIVER_CLASS"));
            return DriverManager.getConnection(properties.getProperty("MYSQL_URL"), properties.getProperty
                    ("MYSQL_USER"), properties.getProperty("MYSQL_PASSWORD"));
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Connection getInstance() {
        if (connection == null) {
            connection = getConnection();
        }
        return connection;
    }

    public static void close(Connection connection, Statement statement, ResultSet resultSet) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
