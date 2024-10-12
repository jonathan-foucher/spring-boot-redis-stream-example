package com.jonathanfoucher.redisstreamexample.services;

import com.jonathanfoucher.redisstreamexample.data.JobDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class JobConsumer implements StreamListener<String, ObjectRecord<String, JobDto>> {
    private final RedisTemplate<String, String> redisTemplate;

    @Override
    public void onMessage(ObjectRecord<String, JobDto> jobRecord) {
        JobDto jobMessage = jobRecord.getValue();
        processJob(jobMessage);

        redisTemplate.opsForStream()
                .delete(jobRecord);
    }

    private void processJob(JobDto job) {
        log.info("starting to process job {}", job);
        try {
            // simulate running job
            TimeUnit.SECONDS.sleep(10);
            log.info("successfully processed job {}", job);
        } catch (InterruptedException e) {
            log.error("failed to process job {}", job);
            log.error(e.getMessage());
            Thread.currentThread().interrupt();
        }
    }
}
