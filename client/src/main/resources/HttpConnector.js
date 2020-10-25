function send(url, method, payload) {
    var xhttp = new XMLHttpRequest();
    xhttp.open(method, url, false);
    if (method != "GET") {
        xhttp.setRequestHeader("Content-type", "application/json");
    }
    var data = payload != null ? JSON.stringify(payload) : null;
    try {
        xhttp.send(data);
        return JSON.parse(xhttp.responseText);
    } catch (e) {
        throw e;
    }
}

function post(url, payload) {
    return send(url, "POST", payload)
}

function get(url) {
    return send(url, "GET", null)
}

function put(url, payload) {
    return send(url, "PUT", payload)
}


function buildUrl(prefix, suffix) {
    if (suffix == null || "" == suffix) {
        return prefix;
    }
    prefix = prefix.endsWith("/") ? prefix.substring(0, prefix.length() - 1) : prefix;
    suffix = suffix.startsWith("/") ? suffix.substring(1) : suffix;
    return prefix + "/" + suffix;
}

var DriverManager = {
    getConnection: function(url, info) {
        if (url == null) {
            throw "The url cannot be null";
        }
        var connection = new HttpDriver().connect(url, info);
        if (connection == null) {
            throw "No suitable driver found for " + url;
        }
        return connection;
    }
}

function HttpDriver() {
    this.connect = function(url, info) {
        return this.acceptsURL(url) ? new Connection(post(buildUrl(getHttpUrl(url), "connection"), getConnectionInfo(url, info))) : null;
    }

    this.acceptsURL = function(url) {
        return url != null && (url.startsWith("http:") || url.startsWith("https:")) && post(buildUrl(getHttpUrl(url), "acceptsurl"), url);
    }

    function getHttpUrl(url) {
        return url.split("#")[0];
    }

    function getConnectionInfo(url, info) {
        var parts = url.split("#", 2);
        var jdbcUrl = parts.length > 1 ? parts[1] : null;
        return {
            url: jdbcUrl,
            properties: info
        }
    }
}

function Connection(proxy) {
    this.entityUrl = proxy.entityUrl;
    if (this.entityUrl == null) {
        throw "Cannot create connection"
    }
    this.close = function() {
        send(this.entityUrl, "DELETE", null);
    }
}
