package com.jonathanfoucher.redisstreamexample.services;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.jonathanfoucher.redisstreamexample.data.JobDto;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.time.Instant;
import java.util.List;

import static ch.qos.logback.classic.Level.INFO;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

@SpringJUnitConfig(JobConsumer.class)
class JobConsumerTest {
    @Autowired
    private JobConsumer jobConsumer;
    @MockitoBean
    private RedisTemplate<String, String> redisTemplate;
    @MockitoBean
    private StreamOperations<String, Object, Object> streamOperations;

    private static final String STREAM_NAME = "job_queue";
    private static final Long JOB_ID = 15L;
    private static final String JOB_NAME = "some job name";
    private static final String RECORD_ID = Instant.now().minusSeconds(600).toEpochMilli() + "-0";

    @Test
    void onMessageReceived() {
        // GIVEN
        Logger log = (Logger) LoggerFactory.getLogger(JobConsumer.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        log.addAppender(listAppender);

        JobDto job = initJobDto();
        RecordId recordId = RecordId.of(RECORD_ID);
        ObjectRecord<String, JobDto> jobRecord = ObjectRecord.create(STREAM_NAME, job)
                .withId(recordId);

        when(redisTemplate.opsForStream())
                .thenReturn(streamOperations);

        // WHEN
        jobConsumer.onMessage(jobRecord);

        // THEN
        ArgumentCaptor<ObjectRecord<String, JobDto>> capturedJobRecord = ArgumentCaptor.forClass(ObjectRecord.class);
        verify(streamOperations, times(1)).delete(capturedJobRecord.capture());

        assertEquals(recordId, capturedJobRecord.getValue().getId());
        checkJob(capturedJobRecord.getValue().getValue());

        List<ILoggingEvent> logs = listAppender.list;
        assertEquals(2, logs.size());

        assertEquals(INFO, logs.get(0).getLevel());
        assertEquals("starting to process job " + job, logs.get(0).getFormattedMessage());

        assertEquals(INFO, logs.get(1).getLevel());
        assertEquals("successfully processed job " + job, logs.get(1).getFormattedMessage());
    }

    private JobDto initJobDto() {
        JobDto job = new JobDto();
        job.setId(JOB_ID);
        job.setName(JOB_NAME);
        return job;
    }

    private void checkJob(JobDto job) {
        assertNotNull(job);
        assertEquals(JOB_ID, job.getId());
        assertEquals(JOB_NAME, job.getName());
    }
}
