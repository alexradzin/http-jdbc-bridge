package com.nosqldriver.jdbc.http;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.util.Optional.ofNullable;

public class ReloadableClassLoader extends URLClassLoader {
    private static final Predicate<File> JAR_PREDICATE = f -> f.isFile() && f.getName().endsWith(".jar");
    private static final FileFilter JAR_FILTER = JAR_PREDICATE::test;
    private final AtomicBoolean watch;

    private static class WatchEventProcessor implements Runnable {
        private final WatchService watcher;
        private final File dir;
        private final AtomicBoolean watch;
        private final ReloadableClassLoader reloadableClassLoader;

        private WatchEventProcessor(WatchService watcher, File dir, AtomicBoolean watch, ReloadableClassLoader reloadableClassLoader) {
            this.watcher = watcher;
            this.dir = dir;
            this.watch = watch;
            this.reloadableClassLoader = reloadableClassLoader;
        }

        @Override
        public void run() {
            while(watch.get()) {
                try {
                    WatchKey key = watcher.take();
                    if (!watch.get()) {
                        return;
                    }
                    //noinspection unchecked
                    key.pollEvents().stream()
                            .filter(e -> watch.get())
                            .filter(e -> ENTRY_CREATE.equals(e.kind()))
                            .map(e -> new File(dir, (((WatchEvent<Path>)e).context()).toFile().getName()))
                            .filter(JAR_PREDICATE)
                            .map(ReloadableClassLoader::toUrl)
                            .forEach(reloadableClassLoader::addURL);
                } catch (InterruptedException e) {
                    // ignore
                } catch (ClosedWatchServiceException e) {
                    return;
                }
            }
        }
    }

    public ReloadableClassLoader(String[] names, ClassLoader parent) {
        super(Stream.concat(Arrays.stream(classpath()), Arrays.stream(jars(names))).toArray(URL[]::new), parent);
        watch = new AtomicBoolean(true);
        Stream.of(names).map(File::new).filter(f -> f.exists() && f.isDirectory()).forEach(dir -> createWatching(dir, watch, this));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> watch.set(false)));
    }

    private static URL[] classpath() {
        return Stream.of(System.getProperty("java.class.path").split(File.pathSeparator))
                .map(File::new)
                .map(ReloadableClassLoader::toUrl)
                .toArray(URL[]::new);
    }

    private static URL[] jars(String[] names) {
        return Stream.of(names)
                .map(File::new)
                .flatMap(dir -> Arrays.stream(ofNullable(dir.listFiles(JAR_FILTER)).orElse(new File[0])))
                .map(ReloadableClassLoader::toUrl)
                .toArray(URL[]::new);
    }

    private static URL toUrl(File file) {
        try {
            return file.toURI().toURL();
        } catch (MalformedURLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void createWatching(File dir, AtomicBoolean watch, ReloadableClassLoader reloadableClassLoader) {
        try {
            WatchService watcher = FileSystems.getDefault().newWatchService();
            dir.toPath().register(watcher, ENTRY_CREATE);
            Thread worker = new Thread(new WatchEventProcessor(watcher, dir, watch, reloadableClassLoader));
            worker.setDaemon(true);
            worker.start();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected void addURL(URL url) {
        super.addURL(url);
    }
}
