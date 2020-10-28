function send(url, method, payload, callback) {
    var xhttp = new XMLHttpRequest();
    var async = callback ? true : false
    xhttp.open(method, url, async);
    if (method != "GET") {
        xhttp.setRequestHeader("Content-type", "application/json");
    }
    var data = payload != null ? JSON.stringify(payload) : null;
    try {
        if (async) {
            xhttp.onload = function() {
                callback(throwIfError(JSON.parse(xhttp.responseText)));
            }
            xhttp.send(data);
        } else {
            xhttp.send(data);
            return throwIfError(JSON.parse(xhttp.responseText));
        }
    } catch (e) {
        throw e;
    }
}

function post(url, payload, callback) {
    return send(url, "POST", payload, callback)
}

function get(url, callback) {
    return send(url, "GET", null, callback)
}

function put(url, payload, callback) {
    return send(url, "PUT", payload, callback)
}


function buildUrl(prefix, suffix) {
    if (suffix == null || "" == suffix) {
        return prefix;
    }
    prefix = prefix.endsWith("/") ? prefix.substring(0, prefix.length() - 1) : prefix;
    suffix = suffix.startsWith("/") ? suffix.substring(1) : suffix;
    return prefix + "/" + suffix;
}

function throwIfError(obj) {
    if (obj.className && obj.message) {
        throw obj.className + ": " + obj.message;
    }
    return obj;
}

var DriverManager = {
    getConnection: function(url, info, callback) {
        if (url == null) {
            throw "The url cannot be null";
        }
        if (callback) {
            new HttpDriver().connect(url, info, callback);
        } else {
            var connection = new HttpDriver().connect(url, info);
            if (connection == null) {
                throw "No suitable driver found for " + url;
            }
            return connection;
        }
    }
}

function HttpDriver() {
    this.connect = function(url, info, callback) {
        if (callback) {
            this.acceptsURL(url, function(accepts) {
                if (accepts) {
                    post(buildUrl(getHttpUrl(url), "connection"), getConnectionInfo(url, info), function(proxy) {
                        callback(new Connection(proxy))
                    });
                } else {
                    throw "URL " + url + " is unsupported";
                }
            })
        } else {
            return this.acceptsURL(url) ? new Connection(post(buildUrl(getHttpUrl(url), "connection"), getConnectionInfo(url, info))) : null;
        }
    }

    this.acceptsURL = function(url, callback) {
        return url != null && (url.startsWith("http:") || url.startsWith("https:")) && post(buildUrl(getHttpUrl(url), "acceptsurl"), url, callback);
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

    this.createStatement = function(callback) {
        if (callback) {
            post(this.entityUrl + "/statement", null, function(proxy) {
                callback(new Statement(proxy, this.Connection))
            })
        } else {
            return new Statement(post(this.entityUrl + "/statement", null), this)
        }
    },

    this.close = function() {
        send(this.entityUrl, "DELETE", null);
    }
}

function Statement(proxy, connection) {
    this.entityUrl = proxy.entityUrl;
    if (this.entityUrl == null) {
        throw "Cannot create statement"
    }
    this.getConnection = function() {
        return connection;
    }

    this.executeQuery = function(sql, callback) {
        if (callback) {
            post(this.entityUrl + "/query", sql, function(proxy) {
                callback(new ResultSet(proxy, this.Statement))
            })
        } else {
            return new ResultSet(post(this.entityUrl + "/query", null), this)
        }
    },

    this.executeUpdate = function(sql, callback) {
        return post(this.entityUrl + "/update", sql, callback)
    },

    this.getMaxFieldSize = function(callback) {
        return get(this.entityUrl + "/maxfieldsize", callback)
    },

    this.setMaxFieldSize = function(max, callback) {
        return post(this.entityUrl + "/maxfieldsize", max, callback)
    },

    this.getMaxRows = function(callback) {
        return get(this.entityUrl + "/maxrows", callback)
    },

    this.setMaxRows = function(max, callback) {
        return post(this.entityUrl + "/maxrows", max, callback)
    },

    this.setEscapeProcessing = function(enable, callback) {
        return post(this.entityUrl + "/escapeprocessing", enable, callback)
    },

    this.getQueryTimeout = function(callback) {
        return get(this.entityUrl + "/querytimeout", callback)
    },

    this.setQueryTimeout = function(seconds, callback) {
        return post(this.entityUrl + "/querytimeout", seconds, callback)
    },

    this.cancel = function() {
        send(this.entityUrl + "/cancel", "DELETE", null);
    },

    this.getWarnings = function(callback) {
        return get(this.entityUrl + "/warnings", callback)
    },

    this.clearWarnings = function(callback) {
        return send(this.entityUrl + "/warnings", "DELETE", null);
    },

    this.setCursorName = function(name) {
        return post(this.entityUrl + "/cursorname", name)
    },

    this.execute = function(sql, callback) {
        return post(this.entityUrl + "/execute", sql, callback)
    },

    this.getResultSet = function(callback) {
         if (callback) {
             get(this.entityUrl + "/resultset", function(proxy) {
                 callback(new ResultSet(proxy, this.Statement))
             })
         } else {
             return new ResultSet(get(this.entityUrl + "/resultset"), this)
         }
    },

    this.getUpdateCount = function(callback) {
        return get(this.entityUrl + "/updatecount", callback)
    },

    this.getMoreResults = function(callback) {
        return get(this.entityUrl + "/more", callback)
    },

    this.setFetchDirection = function(callback) {
        return get(this.entityUrl + "/fetch/direction", callback)
    },

    this.setFetchDirection = function(direction, callback) {
        return post(this.entityUrl + "/fetch/direction", direction, callback)
    },

    this.getFetchSize = function(callback) {
        return get(this.entityUrl + "/fetch/size", callback)
    },

    this.setFetchSize = function(rows, callback) {
        return post(this.entityUrl + "/fetch/size", rows, callback);
    },

    this.getResultSetConcurrency = function(callback) {
        return get(this.entityUrl + "/resultset/concurrency", callback);
    },

    this.getResultSetType = function(callback) {
        return get(this.entityUrl + "/resultset/type", callback);
    },

    this.addBatch = function(sql) {
        post(this.entityUrl + "/batch", sql);
    },

    this.clearBatch = function(sql) {
        send(this.entityUrl + "/batch", "DELETE", null);
    },

    this.executeBatch = function(callback) {
        post(this.entityUrl + "/batch", null, callback);
    },

    this.getMoreResults = function(current, callback) {
        get(this.entityUrl + "/more?current=" + current, callback);
    },

    this.getGeneratedKeys = function(callback) {
         if (callback) {
             get(this.entityUrl + "/generatedkeys", function(proxy) {
                 callback(new ResultSet(proxy), this.Statement)
             })
         } else {
             return new ResultSet(get(this.entityUrl + "/generatedkeys"), this)
         }
    },

    this.getResultSetHoldability = function(callback) {
        get(this.entityUrl + "/resultset/holdability", callback);
    },

    this.isClosed = function(callback) {
        get(this.entityUrl + "/closed", callback);
    },

    this.isPoolable = function(callback) {
        return get(this.entityUrl + "/poolable", callback)
    },

    this.setPoolable = function(poolable) {
        return post(this.entityUrl + "/poolable", poolable);
    },


    this.isCloseOnCompletion = function(callback) {
        return get(this.entityUrl + "/closeoncompletion", callback)
    },

    this.setCloseOnCompletion = function() {
        post(this.entityUrl + "/closeoncompletion", null);
    },

    this.getLargeUpdateCount = function(callback) {
        return get(this.entityUrl + "/large/updatecount", callback)
    },

    this.getLargeMaxRows = function(callback) {
        return get(this.entityUrl + "/large/maxrows", callback)
    },

    this.setLargeMaxRows = function() {
        post(this.entityUrl + "/large/maxrows", null);
    },

    this.executeLargeBatch = function(callback) {
        return post(this.entityUrl + "/large/batch", null, callback);
    },

    this.executeUpdate = function() {
        var allArgs = arguments;
        allArgs.unshift("update");
        return execute(allArgs)
    },

    this.executeLargeUpdate = function() {
        var allArgs = arguments;
        allArgs.unshift("large/update");
        return execute(allArgs)
    },

    this.execute = function() {
        var allArgs = arguments;
        allArgs.unshift("execute");
        return execute(allArgs)
    },

    this.enquoteLiteral = function(val, callback) {
        return post(this.entityUrl + "/enquote/literal", val, callback);
    },

    this.enquoteIdentifier = function(identifier, alwaysQuote, callback) {
        return post(this.entityUrl + "/enquote/identifier/" + alwaysQuote, identifier, callback);
    },

    this.isSimpleIdentifier = function(identifier, callback) {
        return get(this.entityUrl + "/simple/identifier/" + encode(identifier), callback);
    },

    this.enquoteNCharLiteral = function(val, callback) {
        return post(this.entityUrl + "/enquote/nchar/literal", val, callback);
    },

    this.close = function() {
        send(this.entityUrl, "DELETE", null);
    }
    ///////////////////////

    // prefix, sql, callback
    // prefix, sql, autoGeneratedKeys, callback
    // prefix, sql, columnIndexes
    // prefix, sql, columnNames
    function execute() {
        var prefix = arguments[0];
        var sql = null;
        var autoGeneratedKeys = null;
        var callback = null;
        var columnIndexes = null;
        var columnNames = null;
        if (arguments.length == 4) {
            sql = arguments[1];
            if(typeof arguments[2] == 'number') {
                autoGeneratedKeys = arguments[2];
            } else if (Array.isArray(arguments[2])) {
                if (arguments[3].length == 0) {
                    columnIndexes = new Array();
                } else {
                    var arg0 = arguments[2][0];
                    if (typeof arg0 == 'number') {
                        columnIndexes = arguments[2];
                    } else {
                        columnNames = arguments[2];
                    }
                }
            }
            callback = arguments[3];
        } else if (arguments.length == 3) { // sql, callback or sql, autoGeneratedKeys
            if(typeof arguments[2] == 'number') {
                sql = arguments[1];
                autoGeneratedKeys = arguments[2];
            } else {
                sql = arguments[1];
                callback = arguments[2];
            }
        }

        var urlSuffix = "";
        if (typeof autoGeneratedKeys == 'number') {
            urlSuffix = "?keys=" + autoGeneratedKeys;
        } else if (columnIndexes != null) {
            urlSuffix = "?indexes=" + columnIndexes.join(",");
        } else if (columnNames != null) {
            urlSuffix = "?names=" + columnNames.map(name => encode(name)).join(",");
        }

        var urlSuffix = typeof autoGeneratedKeys == 'number' ? "?keys=" + autoGeneratedKeys : "";
        return post(this.entityUrl + "/" + prefix + urlSuffix, sql, callback);
    }
}


function ResultSet(proxy, statement) {
    this.entityUrl = proxy.entityUrl;
    if (this.entityUrl == null) {
        throw "Cannot create result set";
    }
    this.valueWasNull = false;
    this.current = null;
    this.metadata = null;

    this.getStatement = function() {
        return statement;
    },

    this.next = function(callback) {
          if (callback) {
              get(this.entityUrl + "/nextrow", function(proxy) {
                  this.current = new RowData(proxy);
                  callback(new ResultSet(this.current))
              })
          } else {
              this.current = new RowData(get(this.entityUrl + "/nextrow"))
              return this.current.isMoved();
          }
    },

    this.wasNull = function() {
        return this.valueWasNull;
    },

    this.getString = function(index) {
        return cast(this.getObject(index), 'string');
    },

    this.getNumber = function(index) {
        return cast(this.getObject(index), 'number');
    },

    this.getBoolean = function(index) {
        return cast(this.getObject(index), 'boolean');
    },

    this.getObject = function(index) {
        var value = this.current.getRow()[index - 1];
        this.valueWasNull = value == null;
        return value;
    },

    this.getMetaData = function(callback) {
        if (callback) {
            if(this.metadata == null) {
                get(this.entityUrl + "/metadata", function(proxy) {
                   this.metadata = new ResultSetMetadata(proxy);
                   callback(metadata)
                })
            } else {
                callback(metadata);
            }
        } else {
            if(this.metadata == null) {
                this.metadata = new ResultSetMetadata(get(this.entityUrl + "/metadata"))
            }
            return this.metadata;
        }
   }

    this.close = function() {
        send(this.entityUrl, "DELETE", null);
    }

    function cast(value, type) {
        if (value == null) {
            return value;
        }
        if(typeof value == type) {
            return value;
        }
        if (type == 'string') {
            return value.toString();
        }
        throw "Cannot cast " + value + " to " + type;
    }
}

function RowData(proxy) {
    this.isMoved = function() {
        return proxy.moved;
    },
    this.getRow = function() {
        return proxy.row;
    }
}

function ResultSetMetadata(proxy) {
    var columns = proxy.columns;

    // additional functions not defined in java interface
    this.getColumn = function(index) {return columns[index - 1]},
    this.getColumns = function() {return columns},
    // standard JDBC functions
    this.getColumnCount = function() {return columns.length},
    this.isAutoIncrement = function(column) {return this.getColumn(column).autoIncrement},
    this.isCaseSensitive = function(column) {return this.getColumn(column).caseSensitive},
    this.isSearchable = function(column) {return this.getColumn(column).searchable},
    this.isCurrency = function(column) {return this.getColumn(column).currency},
    this.isNullable = function(column) {return this.getColumn(column).nullable},
    this.isSigned = function(column) {return this.getColumn(column).signed},
    this.getColumnDisplaySize = function(column) {return this.getColumn(column).displaySize},
    this.getColumnLabel = function(column) {return this.getColumn(column).label},
    this.getColumnName = function(column) {return this.getColumn(column).name},
    this.getSchemaName = function(column) {return this.getColumn(column).schema},
    this.getPrecision = function(column) {return this.getColumn(column).precision},
    this.getScale = function(column) {return this.getColumn(column).scale},
    this.getTableName = function(column) {return this.getColumn(column).table},
    this.getCatalogName = function(column) {return this.getColumn(column).catalog},
    this.getColumnType = function(column) {return this.getColumn(column).type},
    this.getColumnTypeName = function(column) {return this.getColumn(column).typeName},
    this.isReadOnly = function(column) {return this.getColumn(column).readOnly},
    this.isWritable = function(column) {return this.getColumn(column).writable},
    this.isDefinitelyWritable = function(column) {return this.getColumn(column).definitelyWritable},
    this.getColumnClassName = function(column) {return this.getColumn(column).className}
}

