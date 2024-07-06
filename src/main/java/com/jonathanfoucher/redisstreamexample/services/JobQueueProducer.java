package com.jonathanfoucher.redisstreamexample.services;

import com.jonathanfoucher.redisstreamexample.data.JobDto;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import static java.util.Objects.isNull;

@Service
@RequiredArgsConstructor
public class JobQueueProducer {
    private final RedisTemplate<String, String> redisTemplate;

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Value("${redis-stream-example.stream-key}")
    private String streamKey;

    public RecordId produce(JobDto jobMessage) {
        ObjectRecord<String, JobDto> record = StreamRecords.newRecord()
                .ofObject(jobMessage)
                .withStreamKey(streamKey);

        RecordId recordId = redisTemplate.opsForStream()
                .add(record);

        if (isNull(recordId)) {
            log.error("error producing message for job {}", jobMessage);
            return null;
        }

        log.info("job {} was added to the queue with id {}", jobMessage, recordId);
        return recordId;
    }
}
