package persistence;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Manages database connections using HikariCP connection pool.
 * Singleton pattern ensures only one connection pool exists.
 */
public class DatabaseManager {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseManager.class);
    private static DatabaseManager instance;
    private HikariDataSource dataSource;
    private boolean initialized = false;

    private DatabaseManager() {
        // Private constructor for singleton
    }

    /**
     * Gets the singleton instance of DatabaseManager.
     */
    public static synchronized DatabaseManager getInstance() {
        if (instance == null) {
            instance = new DatabaseManager();
        }
        return instance;
    }

    /**
     * Initializes the database connection pool.
     * Should be called once at application startup.
     */
    public synchronized void initialize() {
        if (initialized) {
            logger.warn("DatabaseManager already initialized");
            return;
        }

        try {
            Properties props = loadDatabaseProperties();

            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(props.getProperty("db.url"));
            config.setUsername(props.getProperty("db.username"));
            config.setPassword(props.getProperty("db.password"));

            // Connection pool settings
            config.setMaximumPoolSize(Integer.parseInt(
                    props.getProperty("db.pool.maximumPoolSize", "10")));
            config.setMinimumIdle(Integer.parseInt(
                    props.getProperty("db.pool.minimumIdle", "2")));
            config.setConnectionTimeout(Long.parseLong(
                    props.getProperty("db.pool.connectionTimeout", "30000")));
            config.setIdleTimeout(Long.parseLong(
                    props.getProperty("db.pool.idleTimeout", "600000")));
            config.setMaxLifetime(Long.parseLong(
                    props.getProperty("db.pool.maxLifetime", "1800000")));

            // Performance settings
            config.addDataSourceProperty("cachePrepStmts", "true");
            config.addDataSourceProperty("prepStmtCacheSize", "250");
            config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

            dataSource = new HikariDataSource(config);
            initialized = true;

            logger.info("Database connection pool initialized successfully");

            // Test connection
            try (Connection conn = dataSource.getConnection()) {
                logger.info("Database connection test successful");
            }

        } catch (Exception e) {
            logger.warn("Database connection failed (using file-based logging): {}", e.getMessage());
            // Do not throw exception, allow fallback to file logging
            // throw new RuntimeException("Database initialization failed", e);
        }
    }

    /**
     * Loads database properties from configuration file.
     */
    private Properties loadDatabaseProperties() throws IOException {
        Properties props = new Properties();

        try (InputStream input = getClass().getClassLoader()
                .getResourceAsStream("config/database.properties")) {

            if (input == null) {
                logger.warn("database.properties not found, using defaults");
                // Set default properties
                props.setProperty("db.url", "jdbc:postgresql://localhost:5432/trading_platform");
                props.setProperty("db.username", "postgres");
                props.setProperty("db.password", "postgres");
                return props;
            }

            props.load(input);
        }

        return props;
    }

    /**
     * Gets a database connection from the pool.
     */
    public Connection getConnection() throws SQLException {
        if (!initialized) {
            throw new IllegalStateException("DatabaseManager not initialized. Call initialize() first.");
        }
        return dataSource.getConnection();
    }

    /**
     * Gets the DataSource for advanced usage.
     */
    public DataSource getDataSource() {
        if (!initialized) {
            throw new IllegalStateException("DatabaseManager not initialized. Call initialize() first.");
        }
        return dataSource;
    }

    /**
     * Checks if database is initialized and connected.
     */
    public boolean isInitialized() {
        return initialized;
    }

    /**
     * Shuts down the connection pool.
     * Should be called at application shutdown.
     */
    public synchronized void shutdown() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            logger.info("Database connection pool shut down");
        }
        initialized = false;
    }
}
