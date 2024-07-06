package com.jonathanfoucher.redisstreamexample.errors;

public class JobAlreadyQueuedException extends RuntimeException {
    public JobAlreadyQueuedException(Long jobId) {
        super("Job with id " + jobId + " is already queued");
    }
}
