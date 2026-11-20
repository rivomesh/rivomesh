package io.github.rivomesh;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.ServiceLoader;

public final class ExternalVendorJdbcAcceptance {
    private record Provider(String type, String url) {}

    private static final Provider[] PROVIDERS = {
            new Provider("com.mysql.cj.jdbc.Driver", "jdbc:mysql://127.0.0.1:1/fixture"),
            new Provider("io.github.rivomesh.gaussdb.jdbc.Driver", "jdbc:postgresql://127.0.0.1:1/fixture"),
            new Provider("com.gbase8c.Driver", "jdbc:gbase8c://127.0.0.1:1/fixture"),
            new Provider("com.kingbase8.Driver", "jdbc:kingbase8://127.0.0.1:1/fixture"),
            new Provider("dm.jdbc.driver.DmDriver", "jdbc:dm://127.0.0.1:1"),
    };

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }

    public static final class InaccessibleDriver extends org.postgresql.Driver {
        private InaccessibleDriver() {}
    }

    public static final class MissingDependencyDriver extends org.postgresql.Driver {
        public MissingDependencyDriver() {
            var failure = new NoClassDefFoundError("JdbcFixtureDependency");
            failure.initCause(new IOException("Failed to read the driver dependency"));
            throw failure;
        }
    }

    public static final class NewerRuntimeDriver extends org.postgresql.Driver {
        public NewerRuntimeDriver() {
            throw new UnsupportedClassVersionError("JDBC fixture dependency requires a newer JVM");
        }
    }

    private static void refused(String type, String url, String code) throws Exception {
        try (var ignored = JdbcConnections.open(type, url, "", "")) {
            throw new IllegalStateException("Incompatible driver accepted a connection");
        } catch (JdbcConnections.DriverFailure failure) {
            require("0A000".equals(failure.getSQLState()) && code.equals(failure.getMessage()),
                    "Unexpected JDBC diagnostic for " + type);
            require(code.equals(Sql.targetErrorCode(failure))
                    && code.equals(io.github.rivomesh.platform.TaskService.safeCode(new IllegalStateException(failure)))
                    && !Sql.retryable(failure), "Driver error lost its platform diagnostic");
        }
    }

    private static void absent(Provider provider) throws Exception {
        try {
            Class.forName(provider.type());
            throw new IllegalStateException("Bundled JDBC provider: " + provider.type());
        } catch (ClassNotFoundException expected) {
            // Even the regular connection helper must report the missing provider without linking it.
        }
        refused(provider.type(), provider.url(), "JDBC_DRIVER_UNAVAILABLE");
    }

    private static void supplied(Provider provider, Path expectedFile) throws Exception {
        Class<?> type = Class.forName(provider.type());
        Path loadedFile = Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI()).toRealPath();
        require(loadedFile.equals(expectedFile.toRealPath()), "Unexpected JDBC class origin for " + provider.type());
        Driver driver = ServiceLoader.load(Driver.class).stream().filter(item -> item.type().equals(type))
                .findFirst().orElseThrow(() -> new IllegalStateException("Missing JDBC service: " + provider.type())).get();
        require(driver.acceptsURL(provider.url()), "JDBC provider refuses its configured URL: " + provider.type());
        try (var bytes = new DataInputStream(type.getResourceAsStream('/' + type.getName().replace('.', '/') + ".class"))) {
            require(bytes.readInt() == 0xcafebabe, "Invalid JDBC class file");
            bytes.readUnsignedShort();
            int major = bytes.readUnsignedShort();
            require(major <= 65, "JDBC provider class exceeds Java 21");
            System.out.println("JDBC_SERVICE_ACCEPTED " + provider.type() + " classMajor=" + major);
        }
    }

    private static void core() throws Exception {
        Class.forName("org.postgresql.Driver");
        Class.forName("com.github.shyiko.mysql.binlog.BinaryLogClient");
        try (var connection = JdbcConnections.open("org.h2.Driver", "jdbc:h2:mem:external_jdbc_fixture", "", "");
             var statement = connection.createStatement();
             var result = statement.executeQuery("SELECT 1")) {
            require(result.next() && result.getInt(1) == 1, "Bundled LOCAL journal JDBC failed");
        }
        require("SQL_22000".equals(Sql.targetErrorCode(new SQLException("other provider", "22000", -2106))),
                "Vendor-specific diagnostic leaked into an unrelated SQL exception");
    }

    public static void main(String[] args) throws Exception {
        core();
        String url = "jdbc:postgresql://127.0.0.1:1/fixture";
        refused(InaccessibleDriver.class.getName(), url, "JDBC_DRIVER_UNAVAILABLE");
        refused(MissingDependencyDriver.class.getName(), url, "JDBC_DRIVER_LINKAGE_INVALID");
        refused(NewerRuntimeDriver.class.getName(), url, "JDBC_DRIVER_JAVA_VERSION_UNSUPPORTED");
        System.out.println("JDBC_REFLECTION_AND_LINKAGE_REFUSALS_ACCEPTED");
        require(args.length == 0 || args.length == PROVIDERS.length,
                "Provide all five external driver paths in MySQL, GaussDB, GBase8c, Kingbase, Dameng order");
        for (int index = 0; index < PROVIDERS.length; index++) {
            Provider provider = PROVIDERS[index];
            if (args.length == 0) absent(provider);
            else supplied(provider, Path.of(args[index]));
        }
        if (args.length == 0) {
            System.out.println("EXTERNAL_VENDOR_JDBC_MISSING_DIAGNOSTICS_ACCEPTED");
        } else {
            Class<?> errorType = Class.forName("dm.jdbc.driver.DMException");
            var constructor = errorType.getConstructor(String.class, String.class, int.class, Throwable.class);
            SQLException failure = (SQLException) constructor.newInstance("missing", "22000", -2106, null);
            require("TARGET_TABLE_MISSING_REBUILD_REQUIRED".equals(Sql.targetErrorCode(failure)),
                    "External Dameng exception classification changed");
            System.out.println("EXTERNAL_VENDOR_JDBC_OPERATOR_CLASSPATH_ACCEPTED");
        }
    }
}
