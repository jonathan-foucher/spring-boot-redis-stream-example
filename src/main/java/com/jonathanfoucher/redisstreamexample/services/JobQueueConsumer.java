package com.jonathanfoucher.redisstreamexample.services;

import com.jonathanfoucher.redisstreamexample.data.JobDto;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
public class JobQueueConsumer implements StreamListener<String, ObjectRecord<String, JobDto>> {
    private final RedisTemplate<String, String> redisTemplate;
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Value("${redis-stream-example.stream-key}")
    private String streamKey;

    @Override
    public void onMessage(ObjectRecord<String, JobDto> record) {
        JobDto jobMessage = record.getValue();
        processJob(jobMessage);

        redisTemplate.opsForStream()
                .acknowledge(streamKey, record);
        redisTemplate.opsForStream()
                .delete(record);
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
        }
    }
}
