package com.jonathanfoucher.redisstreamexample.errors;

public class JobAlreadyQueuedException extends RuntimeException {
    public JobAlreadyQueuedException(Long jobId) {
        super("job with id " + jobId + " is already queued");
    }
}
