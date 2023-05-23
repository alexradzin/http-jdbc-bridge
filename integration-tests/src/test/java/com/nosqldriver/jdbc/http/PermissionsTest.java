package com.nosqldriver.jdbc.http;

import com.nosqldriver.jdbc.http.permissions.StatementPermissionsValidatorsConfigurer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.lang.String.join;
import static java.lang.System.lineSeparator;
import static java.nio.file.FileVisitOption.FOLLOW_LINKS;
import static java.util.Comparator.reverseOrder;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class PermissionsTest extends ControllerTestBase {
    private static String jdbcConf;
    private static final String nativeUrl = "jdbc:h2:mem:test";
    private static Path permissionsDir;
    private static final String commonPermissions = join(lineSeparator(), "select * from *", "insert into * (*)", "update *", "delete from *");
    private static final String adminPermissions = join(lineSeparator(), "select * from * limit 100", "insert into * (*)", "update *", "delete from *");
    private static final String userPermissions = join(lineSeparator(), "select * from *", "update *");
    private static final String guestPermissions = join(lineSeparator(), "select * from * where *(=,in)");
    private static final String oneCommonPermissions = join(lineSeparator(), "select i from *", "insert into one (*)", "update one");
    private static final String oneUserPermissions = join(lineSeparator(), "select i from * limit 100");
    private static final String twoUserPermissions = join(lineSeparator(), "select i from * limit 200");

    private static StatementPermissionsValidatorsConfigurer configurer;


    private enum Executor {
        executeQuery {
            @Override
            Object execute(Statement statement, String sql) throws SQLException {
                return statement.executeQuery(sql);
            }
        },
        executeUpdate {
            @Override
            Object execute(Statement statement, String sql) throws SQLException {
                return statement.executeUpdate(sql);
            }
        },
        execute {
            @Override
            Object execute(Statement statement, String sql) throws SQLException {
                return statement.execute(sql);
            }
        },
        ;
        abstract Object execute(Statement statement, String sql) throws SQLException;
    }

    @BeforeAll
    static void beforeAll() throws IOException {
        permissionsDir = Files.createTempDirectory("permissions");
        Path dir1 = Files.createDirectory(permissionsDir.resolve("one"));
        Path dir2 = Files.createDirectory(permissionsDir.resolve("two"));
        System.setProperty("permissions", permissionsDir.toAbsolutePath().toString());
        write(permissionsDir.resolve("permissions.sql"), commonPermissions);
        write(permissionsDir.resolve("admin.permissions.sql"), adminPermissions);
        write(permissionsDir.resolve("user.permissions.sql"), userPermissions);
        write(permissionsDir.resolve("guest.permissions.sql"), guestPermissions);
        write(dir1.resolve("permissions.sql"), oneCommonPermissions);
        write(dir1.resolve("user.permissions.sql"), oneUserPermissions);
        write(dir2.resolve("user.permissions.sql"), twoUserPermissions);
        configurer = new StatementPermissionsValidatorsConfigurer();
        validator = configurer.config();

        enableSecurityAuth();
        jdbcConf = System.setProperty("jdbc.conf", "src/test/resources/jdbc.properties");
        ControllerTestBase.beforeAll();
    }

    @BeforeEach
    @Override
    void initDb(TestInfo testInfo) throws SQLException {
        String user = testInfo.getDisplayName().split(":")[0];
        nativeConn = DriverManager.getConnection(nativeUrl, user, user);
        httpConn = createHttpConnection(user, user);

        String[] before = Stream.of("create.table.all-types.sql", "insert.all-types.sql").map(f -> sqlScript(db(nativeUrl), f)).toArray(String[]::new);
        for (String sql : before) {
            nativeConn.createStatement().execute(sql);
        }
    }

    @AfterEach
    @Override
    void cleanDb() throws SQLException {
        if (nativeConn != null) {
            try (Statement statement = nativeConn.createStatement()) {
                statement.execute("drop table test_all_types");
            }
            nativeConn.close();
        }
        if (httpConn != null) {
            httpConn.close();
        }
    }

    @AfterAll
    static void afterAll() throws IOException {
        ControllerTestBase.afterAll();
        assertTrue(Files.walk(permissionsDir, FOLLOW_LINKS).sorted(Collections.reverseOrder()).map(Path::toFile).allMatch(File::delete));
        configurer.close();
        disableSecurityAuth();
        System.getProperties().remove("permissions");
        if (jdbcConf == null) {
            System.getProperties().remove("jdbc.conf");
        } else {
            System.setProperty("jdbc.conf", jdbcConf);
        }
    }

    private static Connection createHttpConnection(String user, String password) throws SQLException {
        String url = format("%s#%s#properties", httpUrl, nativeUrl);
        return user == null ? DriverManager.getConnection(url) : DriverManager.getConnection(url, user, password);
    }

    @ParameterizedTest(name = "{0}:{2}")
    @CsvSource(delimiter = ';', value = {
            // owner does not have its own permissions.sql, so common permissions are used
            "owner;executeQuery;select * from test_all_types",
            "owner;executeUpdate;insert into test_all_types (i, f) values (1, 3.14)",
            "owner;executeUpdate;update test_all_types set f=1.1415 where i=1",
            "owner;executeUpdate;delete from test_all_types where i=1",

            "admin;executeQuery;select * from test_all_types limit 10",
            "admin;executeUpdate;insert into test_all_types (i, f) values (1, 3.14)",
            "admin;executeUpdate;update test_all_types set f=1.1415 where i=1",
            "admin;executeUpdate;delete from test_all_types where i=1",

            "user;executeQuery;select * from test_all_types",
            "user;executeQuery;select * from test_all_types",
            "user;executeUpdate;update test_all_types set f=1.1415 where i=1",

            "guest;executeQuery;select * from test_all_types where i = 1",
            "guest;executeQuery;select * from test_all_types where i in (1,2,3)",

            "one/owner;executeQuery;select i from test_all_types",
            "one/owner;executeQuery;select i from test_all_types limit 123",
            "one/user;executeQuery;select i from test_all_types limit 100",

            "two/owner;executeQuery;select i from test_all_types",
            "two/owner;executeQuery;select * from test_all_types", // fall back to the common
            "two/user;executeQuery;select i from test_all_types limit 200",
    })
    void success(@SuppressWarnings("unused") /*used in initDb*/ String user, Executor executor, String query) throws SQLException {
        assertNotNull(executor.execute(httpConn.createStatement(), query));
    }

    @ParameterizedTest(name = "{0}:{2}")
    @CsvSource(delimiter = ';', value = {
            "admin;executeQuery;select * from test_all_types;Query must be limited but was not",
            "admin;executeQuery;select * from test_all_types limit 1000;Actual limit 1000 exceeds required one 100",

            "user;executeUpdate;insert into test_all_types (i, f) values (1, 3.14);Statement is not allowed",

            "guest;executeQuery;select * from test_all_types;where clause is required here",
            "guest;executeQuery;select * from test_all_types where i > 10;Condition i > is forbidden",
            "guest;executeUpdate;insert into test_all_types (i, f) values (1, 3.14);Statement is not allowed",
            "guest;executeUpdate;insert into test_all_types (i, f) values (1, 3.14);Statement is not allowed",
            "guest;executeUpdate;delete from test_all_types where i=1;Statement is not allowed",

            "one/owner;executeQuery;select b from test_all_types;Fields [b] cannot be queried",
            "one/owner;executeQuery;select b from test_all_types limit 100;Fields [b] cannot be queried",
            "one/user;executeQuery;select i from test_all_types limit 101;Actual limit 101 exceeds required one 100",
            "two/user;executeQuery;select i from test_all_types limit 201;Actual limit 201 exceeds required one 200",
    })
    void failure(@SuppressWarnings("unused") /*used in initDb*/ String user, Executor executor, String query, String errorMessage) {
        assertEquals(errorMessage, assertThrows(SQLException.class, () -> executor.execute(httpConn.createStatement(), query)).getMessage());
    }

    @Test
    @DisplayName("changes")
    void changes() throws SQLException, IOException {
        httpConn.createStatement().executeQuery("select i from test_all_types");
        write(permissionsDir.resolve("changes.permissions.sql"), "select b from *"); // create new file
        awaitException("Fields [i] cannot be queried", "select i from test_all_types");

        write(permissionsDir.resolve("changes.permissions.sql"), "select * from * limit 10"); // change existing file
        awaitException("Actual limit 20 exceeds required one 10", "select i from test_all_types limit 20");
        httpConn.createStatement().executeQuery("select i from test_all_types limit 10");
        httpConn.createStatement().executeQuery("select * from test_all_types limit 5");

        assertTrue(permissionsDir.resolve("changes.permissions.sql").toFile().delete());
        awaitSuccessfulQuery("select i from test_all_types");
        awaitSuccessfulQuery("select * from test_all_types");
    }

    @Test
    @DisplayName("x/y/nested")
    void nestedChanges() throws IOException, SQLException, InterruptedException {
        httpConn.createStatement().executeQuery("select i from test_all_types");
        Path x = Files.createDirectory(permissionsDir.resolve("x"));
        Thread.sleep(100L);
        Path y = Files.createDirectory(x.resolve("y"));
        Thread.sleep(100L);

        Path conf = y.resolve("permissions.sql");
        write(conf, "select f from *");
        awaitException("Fields [i] cannot be queried", "select i from test_all_types");
        awaitSuccessfulQuery("select f from test_all_types");

        Path confNested = y.resolve("nested.permissions.sql");
        write(confNested, "select b from *"); // create new file
        awaitSuccessfulQuery("select b from test_all_types");
        awaitException("Fields [i] cannot be queried", "select i from test_all_types");
        awaitException("Fields [f] cannot be queried", "select f from test_all_types");

        assertTrue(confNested.toFile().delete());
        awaitSuccessfulQuery("select f from test_all_types");

        assertTrue(conf.toFile().delete());
        awaitSuccessfulQuery("select i from test_all_types");

        assertTrue(y.toFile().delete());
        awaitSuccessfulQuery("select i from test_all_types");

        assertTrue(x.toFile().delete());
        awaitSuccessfulQuery("select i from test_all_types");
    }

    @Test
    @DisplayName("a/b/nested")
    void removingDirectory() throws IOException, SQLException, InterruptedException {
        httpConn.createStatement().executeQuery("select i from test_all_types");
        Path a = Files.createDirectory(permissionsDir.resolve("a"));
        Thread.sleep(100L);
        Path b = Files.createDirectory(a.resolve("b"));
        Thread.sleep(100L);
        write(b.resolve("permissions.sql"), "select f from *");
        awaitException("Fields [i] cannot be queried", "select i from test_all_types");
        awaitSuccessfulQuery("select f from test_all_types");
        write(b.resolve("nested.permissions.sql"), "select b from *");
        Thread.sleep(100L);
        awaitException("Fields [i] cannot be queried", "select i from test_all_types");
        assertEquals(0, Files.walk(a).sorted(reverseOrder()).map(Path::toFile).map(File::delete).mapToInt(d -> d ? 0 : 1).sum());
        awaitSuccessfulQuery("select i from test_all_types");
    }

    private static void write(Path path, String content) throws IOException {
        Files.write(path, content.getBytes());
    }

    private void awaitException(String message, String sql) {
        awaitQuery(sql, rs -> false, e -> message.equals(e.getMessage()));
    }

    private void awaitSuccessfulQuery(String sql) {
        awaitQuery(sql, rs -> true, e -> false);
    }

    private void awaitQuery(String sql, Predicate<ResultSet> rsValidator, Predicate<SQLException> exceptionValidator) {
        await().pollDelay(2, TimeUnit.SECONDS).pollInterval(5, TimeUnit.SECONDS).atMost(30, TimeUnit.SECONDS).until(() -> {
            try {
                return rsValidator.test(httpConn.createStatement().executeQuery(sql));
            } catch(SQLException e) {
                return exceptionValidator.test(e);
            }
        });
    }
}
