package com.atypon.decentraldbcluster.error;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@ControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);


    @ExceptionHandler(Exception.class)
    public final ResponseEntity<Object> handleAllExceptions(Exception ex) {

        ApiError apiError = new ApiError(
                HttpStatus.INTERNAL_SERVER_ERROR,
                ex.getMessage()
        );

        logger.error("Find exception: {}", ex.getMessage(), ex);

        return new ResponseEntity<>(apiError, apiError.getStatus());
    }

    //Bad request handling
    @ExceptionHandler(IllegalArgumentException.class)
    public final ResponseEntity<Object> badRequest(Exception ex) {

        ApiError apiError = new ApiError(
                HttpStatus.BAD_REQUEST,
                ex.getMessage()
        );

        logger.error("Find exception: {}", ex.getMessage(), ex);

        return new ResponseEntity<>(apiError, apiError.getStatus());
    }

    //Not found handling
    @ExceptionHandler(ResourceNotFoundException.class)
    public final ResponseEntity<Object> handleResourceNotFoundException(ResourceNotFoundException ex) {
        ApiError apiError = new ApiError(
                HttpStatus.NOT_FOUND,
                ex.getMessage()
        );

        logger.error("Resource not found: {}", ex.getMessage(), ex);

        return new ResponseEntity<>(apiError, apiError.getStatus());
    }

}

