package com.jonathanfoucher.redisstreamexample.services;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.jonathanfoucher.redisstreamexample.data.JobDto;
import com.jonathanfoucher.redisstreamexample.errors.JobAlreadyQueuedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.List;

import static ch.qos.logback.classic.Level.ERROR;
import static ch.qos.logback.classic.Level.INFO;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SpringJUnitConfig(JobProducer.class)
class JobProducerTest {
    @Autowired
    private JobProducer jobProducer;
    @MockBean
    private RedisTemplate<String, String> redisTemplate;
    @MockBean
    private StreamOperations<String, Object, Object> streamOperations;

    private static final String STREAM_NAME = "job_queue";
    private static final String STREAM_NAME_VAR = "streamKey";
    private static final Long JOB_ID = 15L;
    private static final String JOB_NAME = "some job name";
    private static final String RECORD_ID = Instant.now().toEpochMilli() + "-0";

    @BeforeEach
    void beforeEach() {
        ReflectionTestUtils.setField(jobProducer, STREAM_NAME_VAR, STREAM_NAME);
    }

    @Test
    void produceJobToTheQueue() {
        // GIVEN
        Logger log = (Logger) LoggerFactory.getLogger(JobProducer.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        log.addAppender(listAppender);

        JobDto job = initJobDto();
        RecordId recordId = RecordId.of(RECORD_ID);

        JobDto queuedJob = new JobDto();
        Long queuedJobId = JOB_ID + 10;
        queuedJob.setId(queuedJobId);
        queuedJob.setName("some other job");
        ObjectRecord<String, JobDto> queuedJobRecord = ObjectRecord.create(STREAM_NAME, queuedJob);

        when(redisTemplate.opsForStream())
                .thenReturn(streamOperations);
        when(streamOperations.read(JobDto.class, StreamOffset.fromStart(STREAM_NAME)))
                .thenReturn(List.of(queuedJobRecord));
        when(streamOperations.add((ObjectRecord<String, JobDto>) any()))
                .thenReturn(recordId);

        // WHEN
        String result = jobProducer.produce(job);

        // THEN
        ArgumentCaptor<ObjectRecord<String, JobDto>> capturedRecord = ArgumentCaptor.forClass(ObjectRecord.class);
        verify(redisTemplate, times(2))
                .opsForStream();
        verify(streamOperations, times(1))
                .read(JobDto.class, StreamOffset.fromStart(STREAM_NAME));
        verify(streamOperations, times(1))
                .add(capturedRecord.capture());

        assertEquals(RECORD_ID, result);

        assertEquals(1, capturedRecord.getAllValues().size());
        ObjectRecord<String, JobDto> objectRecord = capturedRecord.getValue();
        assertNotNull(objectRecord.getId());
        assertNotNull(objectRecord.getId().getValue());
        assertEquals("*", objectRecord.getId().getValue());
        checkJob(objectRecord.getValue());

        List<ILoggingEvent> logs = listAppender.list;
        assertEquals(1, logs.size());
        assertEquals(INFO, logs.getFirst().getLevel());
        assertEquals("job " + job + " was added to the queue with id " + RECORD_ID, logs.getFirst().getFormattedMessage());
    }

    @Test
    void produceJobToTheQueueWithFailedToAddJob() {
        // GIVEN
        Logger log = (Logger) LoggerFactory.getLogger(JobProducer.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        log.addAppender(listAppender);

        JobDto job = initJobDto();

        JobDto queuedJob = new JobDto();
        Long queuedJobId = JOB_ID + 10;
        queuedJob.setId(queuedJobId);
        queuedJob.setName("some other job");
        ObjectRecord<String, JobDto> queuedJobRecord = ObjectRecord.create(STREAM_NAME, queuedJob);

        when(redisTemplate.opsForStream())
                .thenReturn(streamOperations);
        when(streamOperations.read(JobDto.class, StreamOffset.fromStart(STREAM_NAME)))
                .thenReturn(List.of(queuedJobRecord));
        when(streamOperations.add((ObjectRecord<String, JobDto>) any()))
                .thenReturn(null);

        // WHEN
        String result = jobProducer.produce(job);

        // THEN
        ArgumentCaptor<ObjectRecord<String, JobDto>> capturedRecord = ArgumentCaptor.forClass(ObjectRecord.class);
        verify(redisTemplate, times(2))
                .opsForStream();
        verify(streamOperations, times(1))
                .read(JobDto.class, StreamOffset.fromStart(STREAM_NAME));
        verify(streamOperations, times(1))
                .add(capturedRecord.capture());

        assertNull(result);

        assertEquals(1, capturedRecord.getAllValues().size());
        ObjectRecord<String, JobDto> objectRecord = capturedRecord.getValue();
        assertNotNull(objectRecord.getId());
        assertNotNull(objectRecord.getId().getValue());
        assertEquals("*", objectRecord.getId().getValue());
        checkJob(objectRecord.getValue());

        List<ILoggingEvent> logs = listAppender.list;
        assertEquals(1, logs.size());
        assertEquals(ERROR, logs.getFirst().getLevel());
        assertEquals("error producing message for job " + job, logs.getFirst().getFormattedMessage());
    }

    @Test
    void produceJobToTheQueueWithJobAlreadyQueued() {
        // GIVEN
        Logger log = (Logger) LoggerFactory.getLogger(JobProducer.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        log.addAppender(listAppender);

        JobDto job = initJobDto();
        RecordId recordId = RecordId.of(RECORD_ID);

        ObjectRecord<String, JobDto> queuedJobRecord = ObjectRecord.create(STREAM_NAME, initJobDto());

        when(redisTemplate.opsForStream())
                .thenReturn(streamOperations);
        when(streamOperations.read(JobDto.class, StreamOffset.fromStart(STREAM_NAME)))
                .thenReturn(List.of(queuedJobRecord));
        when(streamOperations.add((ObjectRecord<String, JobDto>) any()))
                .thenReturn(recordId);

        // WHEN
        assertThatThrownBy(() -> jobProducer.produce(job))
                .isInstanceOf(JobAlreadyQueuedException.class)
                .hasMessageContaining("job with id " + JOB_ID + " is already queued");

        // THEN
        verify(redisTemplate, times(1))
                .opsForStream();
        verify(streamOperations, times(1))
                .read(JobDto.class, StreamOffset.fromStart(STREAM_NAME));
        verify(streamOperations, never())
                .add(any());

        List<ILoggingEvent> logs = listAppender.list;
        assertEquals(0, logs.size());
    }

    @Test
    void getQueuedJobsIds() {
        // GIVEN
        JobDto job = initJobDto();
        ObjectRecord<String, JobDto> jobRecord = ObjectRecord.create(STREAM_NAME, job);

        JobDto job2 = new JobDto();
        Long jobId2 = JOB_ID + 10;
        job2.setId(jobId2);
        job2.setName("some other job");
        ObjectRecord<String, JobDto> jobRecord2 = ObjectRecord.create(STREAM_NAME, job2);

        JobDto job3 = new JobDto();
        Long jobId3 = JOB_ID - 3;
        job3.setId(jobId3);
        job3.setName("some third job");
        ObjectRecord<String, JobDto> jobRecord3 = ObjectRecord.create(STREAM_NAME, job3);

        when(redisTemplate.opsForStream())
                .thenReturn(streamOperations);
        when(streamOperations.read(JobDto.class, StreamOffset.fromStart(STREAM_NAME)))
                .thenReturn(List.of(jobRecord, jobRecord2, jobRecord3));

        // WHEN
        List<Long> results = jobProducer.getQueuedJobsIds();

        // THEN
        verify(redisTemplate, times(1))
                .opsForStream();
        verify(streamOperations, times(1))
                .read(JobDto.class, StreamOffset.fromStart(STREAM_NAME));

        assertNotNull(results);
        assertEquals(3, results.size());
        assertEquals(JOB_ID, results.get(0));
        assertEquals(jobId2, results.get(1));
        assertEquals(jobId3, results.get(2));
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
