package com.jonathanfoucher.redisstreamexample.controllers.advices;

import com.jonathanfoucher.redisstreamexample.errors.JobAlreadyQueuedException;
import com.jonathanfoucher.redisstreamexample.errors.RemovingRunningJobException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
public class GlobalControllerExceptionHandler {
    @ExceptionHandler({JobAlreadyQueuedException.class, RemovingRunningJobException.class, JobAlreadyQueuedException.class})
    public ResponseEntity<String> handleConflict(Exception exception) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(exception.getMessage());
    }
}
