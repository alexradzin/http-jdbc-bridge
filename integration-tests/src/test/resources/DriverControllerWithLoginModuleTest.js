var httpUrl = "http://localhost:8080";
var DriverControllerWithLoginModuleTest = {
    createAndCloseConnectionWithPredefinedUrl: function(url, props) {
        var conn = new HttpDriver().connect(url, props);
        conn.close();
        return conn;
    }
}
