package com.nosqldriver.jdbc.http.permissions;

import com.nosqldriver.util.function.Configuration;
import com.nosqldriver.util.function.ThrowingBiFunction;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.util.stream.Collectors.joining;

public class StatementPermissionsValidatorsConfigurer implements Closeable {
    private static final String PERMISSIONS_CONF_DIR_PROP = "permissions";
    private static final String permissionsSql = "permissions.sql";
    private static final String permissionsSuffix = "." + permissionsSql;
    private final Map<File, WatchEventProcessor> watchProcessors = new HashMap<>();

    private class WatchEventProcessor implements Runnable {
        private final StatementPermissionsValidators validators;
        private final WatchService watcher;
        private final WatchKey key;
        private final File root;
        private final File dir;
        private final boolean discoverSubDirs;
        private volatile boolean watch = true;

        private WatchEventProcessor(StatementPermissionsValidators validators, WatchService watcher, WatchKey key, File root, File dir, boolean discoverSubDirs) {
            this.validators = validators;
            this.watcher = watcher;
            this.key = key;
            this.root = root;
            this.dir = dir;
            this.discoverSubDirs = discoverSubDirs;
        }

        @Override
        public void run() {
            //Yes, this while loop does not have exit condition. It is OK. The loop is infinite but the thread is daemon,
            //so it will run as far as the application is running.
            while (watch) {
                WatchKey key;
                try {
                    key = watcher.take();
                } catch (InterruptedException e) {
                    continue;
                } catch (ClosedWatchServiceException e) {
                    return;
                }
                if (!watch) {
                    return;
                }

                try {
                    for (WatchEvent<?> event : key.pollEvents()) {
                        if (!watch) {
                            return;
                        }
                        WatchEvent.Kind<?> kind = event.kind();
                        @SuppressWarnings("unchecked") WatchEvent<Path> ev = (WatchEvent<Path>) event;
                        Path path = ev.context();
                        String fileName = path.getFileName().toString();
                        File file = new File(dir, path.toFile().getPath());
                        if (file.isDirectory()) {
                            if (ENTRY_CREATE.equals(kind) || ENTRY_MODIFY.equals(kind)) {
                                config(validators, root, file, null, discoverSubDirs);
                            } else if (ENTRY_DELETE.equals(kind)) {
                                String groupName = root.toPath().relativize(dir.toPath()).toString();
                                validators.removeConfiguration(groupName);
                                watchProcessors.remove(dir);
                                String absolutePath = dir.getAbsolutePath();
                                watchProcessors.entrySet().removeIf(entry -> entry.getKey().getAbsolutePath().startsWith(absolutePath + "/"));
                            }
                        } else if (fileName.endsWith(permissionsSuffix)) {
                            String userName = fileName.substring(0, fileName.length() - permissionsSuffix.length());
                            String groupName = root.toPath().relativize(dir.toPath()).toString();
                            String fullUserName = Stream.of(groupName, userName).filter(s -> !"".equals(s)).collect(joining("/"));
                            if (ENTRY_DELETE.equals(kind) || ENTRY_MODIFY.equals(kind)) {
                                validators.removeConfiguration(fullUserName);
                            }
                            if (ENTRY_CREATE.equals(kind) || ENTRY_MODIFY.equals(kind)) {
                                config(validators, root, dir, null, discoverSubDirs);
                            }
                        } else if (fileName.equals(permissionsSql)) {
                            String groupName = root.toPath().relativize(dir.toPath()).toString();
                            String fullUserName = groupName;
                            if (ENTRY_DELETE.equals(kind) || ENTRY_MODIFY.equals(kind)) {
                                validators.removeConfiguration(fullUserName);
                            }
                            if (ENTRY_CREATE.equals(kind) || ENTRY_MODIFY.equals(kind)) {
                                config(validators, root, dir, file, discoverSubDirs);
                            }
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace(); //TODO: add logging
                } finally {
                    key.reset();
                }
            }
        }

        public void stop() throws IOException {
            key.cancel();
            watcher.close();
            watch = false;
        }
    }

    public ThrowingBiFunction<String, String, String, SQLException> config() throws IOException {
        StatementPermissionsValidators validators = new StatementPermissionsValidators();
        String permissionsConfDirName = Configuration.getConfigurationParameter(PERMISSIONS_CONF_DIR_PROP, null);
        boolean discoverSubDirs = false;
        File permissionsConfDir = new File(".");
        if (permissionsConfDirName != null) {
            permissionsConfDir = new File(permissionsConfDirName);
            discoverSubDirs = true;
        }

        File defaultPermissionsFile = new File(permissionsConfDir, permissionsSql);
        return config(validators, permissionsConfDir, permissionsConfDir, defaultPermissionsFile, discoverSubDirs);
    }

    private ThrowingBiFunction<String, String, String, SQLException> config(
            StatementPermissionsValidators validators,
            File permissionsRoot,
            File permissionsConfDir,
            File defaultPermissionsFile,
            boolean discoverSubDirs) throws IOException {

        File localPermissionsFile = new File(permissionsConfDir, permissionsSql);
        if (localPermissionsFile.exists()) {
            defaultPermissionsFile = localPermissionsFile;
        }

        File[] permissionsFiles = permissionsConfDir.listFiles(f -> f.isFile() && f.getName().endsWith(permissionsSuffix));
        if (permissionsFiles != null) {
            for (File file : permissionsFiles) {
                String fileName = file.getName();
                String userName = fileName.substring(0, fileName.length() - permissionsSuffix.length());
                String groupName = permissionsRoot.toPath().relativize(file.getParentFile().toPath()).toString();
                String fullUserName = Stream.of(groupName, userName).filter(s -> !"".equals(s)).collect(joining("/"));
                StatementPermissionsValidator statementValidator = new StatementPermissionsValidator().addConfiguration(new FileInputStream(file));
                validators.addConfiguration(fullUserName, statementValidator);
            }
        }
        if (defaultPermissionsFile != null && defaultPermissionsFile.exists()) {
            validators.addConfiguration(
                    permissionsRoot.toPath().relativize(permissionsConfDir.toPath()).toString(),
                    new StatementPermissionsValidator().addConfiguration(new FileInputStream(defaultPermissionsFile)));
        }

        if (discoverSubDirs) {
            File[] subDirs = permissionsConfDir.listFiles(File::isDirectory);
            if (subDirs != null) {
                for (File dir : subDirs) {
                    config(validators, permissionsRoot, dir, defaultPermissionsFile, discoverSubDirs);
                }
            }
        }

        createWatching(permissionsRoot, permissionsConfDir, validators, discoverSubDirs);
        return validators;
    }

    private void createWatching(File permissionsConfRootDir, File permissionsConfDir, StatementPermissionsValidators validators, boolean discoverSubDirs) throws IOException {
        if (watchProcessors.containsKey(permissionsConfDir)) {
            return;
        }
        WatchService watcher = FileSystems.getDefault().newWatchService();
        WatchKey watchKey = permissionsConfDir.toPath().register(watcher, ENTRY_MODIFY, ENTRY_DELETE, ENTRY_CREATE);
        WatchEventProcessor processor = new WatchEventProcessor(validators, watcher, watchKey, permissionsConfRootDir, permissionsConfDir, discoverSubDirs);
        watchProcessors.put(permissionsConfDir, processor);
        Thread worker = new Thread(processor);
        worker.setDaemon(true);
        worker.start();
    }

    @Override
    public void close() throws IOException {
        for (WatchEventProcessor processor : watchProcessors.values()) {
            processor.stop();
        }
        watchProcessors.clear();
    }
}
