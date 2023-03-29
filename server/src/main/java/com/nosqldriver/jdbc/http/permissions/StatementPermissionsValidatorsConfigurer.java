package com.nosqldriver.jdbc.http.permissions;

import com.nosqldriver.util.function.ThrowingBiFunction;
import com.nosqldriver.util.function.ThrowingFunction;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.sql.SQLException;
import java.util.Optional;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

public class StatementPermissionsValidatorsConfigurer {
    private static final String PERMISSIONS_CONF_DIR_PROP = "permissions";
    private static final ThrowingFunction<String, String, SQLException> allowAll = query -> query;
    private static final String permissionsSql = "permissions.sql";
    private static final String permissionsSuffix = "." + permissionsSql;

    private static class WatchEventProcessor implements Runnable {
        private final StatementPermissionsValidators validators;
        private final WatchService watcher;

        private WatchEventProcessor(StatementPermissionsValidators validators, WatchService watcher) {
            this.validators = validators;
            this.watcher = watcher;
        }

        @Override
        public void run() {
            //Yes, this while loop does not have exit condition. It is OK. The loop is infinite but the thread is daemon,
            //so it will run as far as the application is running.
            //noinspection InfiniteLoopStatement
            while (true) {
                WatchKey key;
                try {
                    key = watcher.take();
                } catch (InterruptedException e) {
                    continue;
                }

                try {
                    for (WatchEvent<?> event : key.pollEvents()) {
                        WatchEvent.Kind<?> kind = event.kind();
                        @SuppressWarnings("unchecked") WatchEvent<Path> ev = (WatchEvent<Path>) event;
                        Path path = ev.context();
                        String fileName = path.getFileName().toString();
                        if (!fileName.endsWith(permissionsSuffix)) {
                            continue;
                        }
                        String userName = fileName.substring(0, fileName.length() - permissionsSuffix.length());
                        if (ENTRY_DELETE.equals(kind) || ENTRY_MODIFY.equals(kind)) {
                            validators.removeConfiguration(userName);
                        }
                        if (ENTRY_CREATE.equals(kind) || ENTRY_MODIFY.equals(kind)) {
                            addConfiguration(validators, userName, path.toFile());
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace(); //TODO: add logging
                } finally {
                    key.reset();
                }
            }
        }
    }

    public ThrowingBiFunction<String, String, String, SQLException> config() throws IOException {
        StatementPermissionsValidators validators = new StatementPermissionsValidators();
        File permissionsConfDir = getConfigurationFile(PERMISSIONS_CONF_DIR_PROP, ".");
        File defaultPermissionsFile = new File(permissionsConfDir, permissionsSql);
        ThrowingFunction<String, String, SQLException> defaultPermissions = defaultPermissionsFile.exists()
                ?
                new StatementPermissionsValidator().addConfiguration(new FileInputStream(defaultPermissionsFile))
                :
                allowAll;
        validators.setDefaultConfiguration(defaultPermissions);
        File[] permissionsFiles = permissionsConfDir.listFiles(f -> f.isFile() && f.getName().endsWith(permissionsSuffix));
        if (permissionsFiles != null) {
            for (File file : permissionsFiles) {
                String fileName = file.getName();
                String userName = fileName.substring(0, fileName.length() - permissionsSuffix.length());
                if (!"".equals(userName)) {
                    StatementPermissionsValidator statementValidator = new StatementPermissionsValidator().addConfiguration(new FileInputStream(file));
                    //if (defaultPermissionsFile.exists()) {
                    //    statementValidator.addConfiguration(new FileInputStream(defaultPermissionsFile));
                    //}
                    validators.addConfiguration(userName, statementValidator);
                }
            }
        }

        createWatching(permissionsConfDir, validators);

        return validators;
    }

    private static File getConfigurationFile(String propertyName, String defaultName) {
        return new File(Optional.ofNullable(System.getProperty(propertyName, System.getenv(propertyName))).orElse(defaultName));
    }

    private void createWatching(File permissionsConfDir, StatementPermissionsValidators validators) throws IOException {
        WatchService watcher = FileSystems.getDefault().newWatchService();
        permissionsConfDir.toPath().register(watcher, ENTRY_MODIFY, ENTRY_DELETE, ENTRY_CREATE);
        Thread worker = new Thread(new WatchEventProcessor(validators, watcher));
        worker.setDaemon(true);
        worker.start();
    }

    private static void addConfiguration(StatementPermissionsValidators validators, String userName, File file) throws IOException {
        validators.addConfiguration(userName, new StatementPermissionsValidator().addConfiguration(new FileInputStream(file)));
    }
}
