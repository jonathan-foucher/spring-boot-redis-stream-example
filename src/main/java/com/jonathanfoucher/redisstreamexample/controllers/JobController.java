package com.jonathanfoucher.redisstreamexample.controllers;

import com.jonathanfoucher.redisstreamexample.data.JobDto;
import com.jonathanfoucher.redisstreamexample.services.JobQueueProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/v1/jobs")
@RequiredArgsConstructor
public class JobController {
    private final JobQueueProducer jobQueueProducer;

    @PostMapping("/start")
    public RecordId startJob(@RequestBody JobDto job) {
        return jobQueueProducer.produce(job);
    }

    @GetMapping("/queued")
    public List<Long> queuedJobs() {
        return jobQueueProducer.getAllQueuedJobIds();
    }
}
