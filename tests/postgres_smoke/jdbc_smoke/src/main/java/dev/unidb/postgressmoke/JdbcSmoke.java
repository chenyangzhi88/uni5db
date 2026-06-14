package dev.unidb.postgressmoke;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public final class JdbcSmoke {
    private JdbcSmoke() {}

    public static void main(String[] args) throws Exception {
        String jdbcUrl = getenv("PG_GATEWAY_POSTGRES_JDBC_URL");
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            String host = getenvOrDefault("PG_GATEWAY_POSTGRES_HOST", "127.0.0.1");
            String port = getenvOrDefault("PG_GATEWAY_POSTGRES_PORT", "55433");
            String database = getenvOrDefault("PG_GATEWAY_POSTGRES_DATABASE", "defaultdb");
            jdbcUrl = "jdbc:postgresql://" + host + ":" + port + "/" + database;
        }
        String user = getenvOrDefault("PG_GATEWAY_POSTGRES_USER", "postgres");
        String password = getenvOrDefault("PG_GATEWAY_POSTGRES_PASSWORD", "");

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DROP TABLE IF EXISTS jdbc_pg_smoke");
            stmt.executeUpdate("CREATE TABLE jdbc_pg_smoke (id INT PRIMARY KEY, name TEXT NOT NULL)");

            try (PreparedStatement insert = conn.prepareStatement("INSERT INTO jdbc_pg_smoke VALUES (?, ?)")) {
                insert.setInt(1, 1);
                insert.setString(2, "alice");
                insert.executeUpdate();
                insert.setInt(1, 2);
                insert.setString(2, "bob");
                insert.executeUpdate();
            }

            try (PreparedStatement query = conn.prepareStatement("SELECT name FROM jdbc_pg_smoke WHERE id = ?")) {
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

            try (ResultSet rs = stmt.executeQuery("SELECT id, name FROM jdbc_pg_smoke ORDER BY id")) {
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

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    + "WHERE table_name = 'jdbc_pg_smoke' ORDER BY ordinal_position")) {
                int rows = 0;
                while (rs.next()) {
                    rows++;
                }
                if (rows != 2) {
                    throw new IllegalStateException("information_schema returned " + rows + " rows, want 2");
                }
            }
        }

        System.out.println("JDBC postgres smoke ok");
    }

    private static String getenv(String key) {
        return System.getenv(key);
    }

    private static String getenvOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
