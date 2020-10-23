var httpUrl = "http://localhost:8080";
var DriverControllerTest = {
    createAndCloseConnection: function(nativeUrl) {
        var conn = DriverManager.getConnection(httpUrl + "#" + nativeUrl, new Object());
        //assertNotNull(conn);
        conn.close();
    },

    createConnectionViaDriverManagerUsingUnsupportedJdbcUrl: function(url) {
        DriverManager.getConnection(url);
    },

    getDriverUsingUnsupportedJdbcUrl: function(url) {
        DriverManager.getConnection(url);
    },

    getUsingUnsupportedJdbcUrlConnectionDirectly: function(url) {
        new HttpDriver().connect(url, null);
    },

    createAndCloseConnectionWithPredefinedUrl: function(url, props) {
        var conn = new HttpDriver().connect(url, props);
        conn.close();
        return conn;
    },

    createAndCloseConnectionWithPredefinedUrlWrongCredentials: function(url, props) {
        return DriverManager.getConnection(url, props)
    },

    createConnectionWithExistingUserNotMappedToDatabase: function(url, props) {
        return DriverManager.getConnection(url, props)
    }
}

