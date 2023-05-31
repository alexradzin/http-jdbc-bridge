package com.nosqldriver.jdbc.http;

import java.net.URL;
import java.sql.Driver;
import java.util.ServiceLoader;

public class ServerLoader {
    public static void main(String[] args) throws ReflectiveOperationException {
        ClassLoader classLoader = new ReloadableClassLoader(new String[] {"lib", "drivers"}, ClassLoader.getPlatformClassLoader()) {
            @Override
            protected void addURL(URL url) {
                super.addURL(url);
                // DriverManager initializes drivers only once and does not have legal API that allows to re-initialize them.
                // So, the easiest way to register JDBC drivers discovered in this new jar file is to load it using ServiceLoader
                // exactly as DriverManager does. Each driver must register itself in static initializer, so it is enough
                // to iterate over the new discovered drivers.
                //noinspection StatementWithEmptyBody
                for (Driver driver : ServiceLoader.load(Driver.class, this)) {
                    // It is enough to "touch" driver
                }
            }
        };
        Thread.currentThread().setContextClassLoader(classLoader);
        classLoader.loadClass("com.nosqldriver.jdbc.http.Server").getMethod("main", String[].class).invoke(null, new Object[] {args});
    }
}
