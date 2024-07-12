package com.jonathanfoucher.redisstreamexample.errors;

public class JobNotFoundInQueueException extends RuntimeException {
    public JobNotFoundInQueueException(Long jobId) {
        super("job with id " + jobId + " is not queued");
    }
}
