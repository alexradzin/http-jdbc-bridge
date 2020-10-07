package com.nosqldriver.jdbc.http;

public class HttpResponse<T> {
    private final int status;
    private final T payload;


    public HttpResponse(int status, T payload) {
        this.status = status;
        this.payload = payload;
    }

    public int getStatus() {
        return status;
    }

    public T getPayload() {
        return payload;
    }
}
