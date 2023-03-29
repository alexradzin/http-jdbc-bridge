package com.nosqldriver.jdbc.http;

import com.nosqldriver.jdbc.http.permissions.StatementPermissionsValidatorsConfigurer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.lang.String.join;
import static java.lang.System.lineSeparator;
import static java.nio.file.FileVisitOption.FOLLOW_LINKS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PermissionsTest extends ControllerTestBase {
    private static final String nativeUrl = "jdbc:h2:mem:test";
    private static Path permissionsDir;
    private static final String commonPermissions = join(lineSeparator(), "select * from *", "insert into * (*)", "update *", "delete from *");
    private static final String adminPermissions = join(lineSeparator(), "select * from * limit 100", "insert into * (*)", "update *", "delete from *");
    private static final String userPermissions = join(lineSeparator(), "select * from *", "update *");
    private static final String guestPermissions = join(lineSeparator(), "select * from * where *(=,in)");


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
        System.setProperty("permissions", permissionsDir.toAbsolutePath().toString());
        write(permissionsDir.resolve("permissions.sql"), commonPermissions);
        write(permissionsDir.resolve("admin.permissions.sql"), adminPermissions);
        write(permissionsDir.resolve("user.permissions.sql"), userPermissions);
        write(permissionsDir.resolve("guest.permissions.sql"), guestPermissions);
        validator = new StatementPermissionsValidatorsConfigurer().config();
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
        try (Statement statement = nativeConn.createStatement()) {
            statement.execute("drop table test_all_types");
        }
        nativeConn.close();
        httpConn.close();
    }

    @AfterAll
    static void afterAll() throws IOException {
        ControllerTestBase.afterAll();
        Assertions.assertTrue(Files.walk(permissionsDir, FOLLOW_LINKS).sorted(Collections.reverseOrder()).map(Path::toFile).allMatch(File::delete));
    }

    private static Connection createHttpConnection(String user, String password) throws SQLException {
        String url = format("%s#%s", httpUrl, nativeUrl);
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
    })
    void failure(@SuppressWarnings("unused") /*used in initDb*/ String user, Executor executor, String query, String errorMessage) {
        assertEquals(errorMessage, assertThrows(SQLException.class, () -> executor.execute(httpConn.createStatement(), query)).getMessage());
    }

    private static void write(Path path, String content) throws IOException {
        Files.createFile(path);
        Files.write(path, content.getBytes());
    }
}
