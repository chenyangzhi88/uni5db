package dev.unidb.mysqlsmoke;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public final class JdbcSmoke {
    private JdbcSmoke() {}

    public static void main(String[] args) throws Exception {
        String jdbcUrl = getenv("PG_GATEWAY_MYSQL_JDBC_URL");
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            String host = getenvOrDefault("PG_GATEWAY_MYSQL_HOST", "127.0.0.1");
            String port = getenvOrDefault("PG_GATEWAY_MYSQL_PORT", "33307");
            String database = getenvOrDefault("PG_GATEWAY_MYSQL_DATABASE", "defaultdb");
            jdbcUrl = "jdbc:mysql://" + host + ":" + port + "/" + database
                + "?sslMode=DISABLED&allowLoadLocalInfile=true&allowPublicKeyRetrieval=true";
        }
        String user = getenvOrDefault("PG_GATEWAY_MYSQL_USER", "root");
        String password = getenvOrDefault("PG_GATEWAY_MYSQL_PASSWORD", "");

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DROP TABLE IF EXISTS jdbc_smoke");
            stmt.executeUpdate("CREATE TABLE jdbc_smoke (id INT PRIMARY KEY, name VARCHAR(32) NOT NULL)");

            try (PreparedStatement insert = conn.prepareStatement("INSERT INTO jdbc_smoke VALUES (?, ?)")) {
                insert.setInt(1, 1);
                insert.setString(2, "alice");
                insert.executeUpdate();
                insert.setInt(1, 2);
                insert.setString(2, "bob");
                insert.executeUpdate();
            }

            try (PreparedStatement query = conn.prepareStatement("SELECT name FROM jdbc_smoke WHERE id = ?")) {
                query.setInt(1, 2);
                try (ResultSet rs = query.executeQuery()) {
                    if (!rs.next()) {
                        throw new IllegalStateException("prepared query returned no rows");
                    }
                    String name = rs.getString(1);
                    if (!"bob".equals(name)) {
                        throw new IllegalStateException("prepared query returned " + name + ", want bob");
                    }
                    if (rs.next()) {
                        throw new IllegalStateException("prepared query returned more than one row");
                    }
                }
            }

            try (ResultSet rs = stmt.executeQuery("SELECT id, name FROM jdbc_smoke ORDER BY id")) {
                ResultSetMetaData meta = rs.getMetaData();
                if (meta.getColumnCount() != 2
                    || !"id".equalsIgnoreCase(meta.getColumnName(1))
                    || !"name".equalsIgnoreCase(meta.getColumnName(2))) {
                    throw new IllegalStateException("unexpected result metadata");
                }
                int rows = 0;
                while (rs.next()) {
                    rows++;
                }
                if (rows != 2) {
                    throw new IllegalStateException("selected " + rows + " rows, want 2");
                }
            }

            try (ResultSet rs = stmt.executeQuery("DESCRIBE jdbc_smoke")) {
                ResultSetMetaData meta = rs.getMetaData();
                if (meta.getColumnCount() != 6) {
                    throw new IllegalStateException("DESCRIBE returned " + meta.getColumnCount() + " columns, want 6");
                }
                int rows = 0;
                while (rs.next()) {
                    rows++;
                }
                if (rows != 2) {
                    throw new IllegalStateException("DESCRIBE returned " + rows + " rows, want 2");
                }
            }
        }

        System.out.println("JDBC mysql smoke ok");
    }

    private static String getenv(String key) {
        return System.getenv(key);
    }

    private static String getenvOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}

