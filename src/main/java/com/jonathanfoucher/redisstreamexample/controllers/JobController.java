package com.jonathanfoucher.redisstreamexample.controllers;

import com.jonathanfoucher.redisstreamexample.data.JobDto;
import com.jonathanfoucher.redisstreamexample.services.JobProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/v1/jobs")
@RequiredArgsConstructor
public class JobController {
    private final JobProducer jobProducer;

    @PostMapping("/start")
    public String startJob(@RequestBody JobDto job) {
        return jobProducer.produce(job);
    }

    @GetMapping("/queued")
    public List<Long> getQueuedJobIds() {
        return jobProducer.getQueuedJobsIds();
    }
}
