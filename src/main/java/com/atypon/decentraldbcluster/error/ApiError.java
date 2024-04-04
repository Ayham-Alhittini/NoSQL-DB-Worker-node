package com.atypon.decentraldbcluster.error;

import org.springframework.http.HttpStatus;

//TODO: there should be query errors as well
public class ApiError {
    private HttpStatus status;
    private int statusCode;

    private String message;

    public ApiError(HttpStatus status, String message) {
        this.status = status;
        this.statusCode = status.value();
        this.message = message;
    }

    public HttpStatus getStatus() {
        return status;
    }
    public int getStatusCode() {
        return statusCode;
    }

    public void setStatus(HttpStatus status) {
        this.status = status;
        statusCode = status.value();
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}

