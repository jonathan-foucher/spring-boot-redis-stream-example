package com.jonathanfoucher.redisstreamexample.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jonathanfoucher.redisstreamexample.controllers.advices.GlobalControllerExceptionHandler;
import com.jonathanfoucher.redisstreamexample.data.JobDto;
import com.jonathanfoucher.redisstreamexample.errors.JobAlreadyQueuedException;
import com.jonathanfoucher.redisstreamexample.services.JobProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(JobController.class)
@SpringJUnitConfig({JobController.class, GlobalControllerExceptionHandler.class})
class JobControllerTest {
    private MockMvc mockMvc;
    @Autowired
    private JobController jobController;
    @Autowired
    private GlobalControllerExceptionHandler globalControllerExceptionHandler;
    @MockBean
    private JobProducer jobProducer;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final String START_JOB_PATH = "/v1/jobs/start";
    private static final String QUEUED_JOBS_PATH = "/v1/jobs/queued";
    private static final String QUEUED_JOB_PATH = "/v1/jobs/{job_id}/queued";
    private static final Long JOB_ID = 15L;
    private static final String JOB_NAME = "some job name";
    private static final String MESSAGE_ID = Timestamp.valueOf(LocalDateTime.now()) + "-0";

    @BeforeEach
    void initEach() {
        mockMvc = MockMvcBuilders.standaloneSetup(jobController)
                .setControllerAdvice(globalControllerExceptionHandler)
                .setMessageConverters(new MappingJackson2HttpMessageConverter())
                .build();
    }

    @Test
    void startJob() throws Exception {
        // GIVEN
        JobDto job = initJobDto();

        when(jobProducer.produce(any()))
                .thenReturn(MESSAGE_ID);

        // WHEN / THEN
        mockMvc.perform(post(START_JOB_PATH)
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(job))
                )
                .andExpect(status().isOk());

        ArgumentCaptor<JobDto> capturedJob = ArgumentCaptor.forClass(JobDto.class);
        verify(jobProducer, times(1))
                .produce(capturedJob.capture());

        assertEquals(1, capturedJob.getAllValues().size());
        checkJob(capturedJob.getValue());
    }

    @Test
    void startJobWhenJobAlreadyQueued() throws Exception {
        // GIVEN
        JobDto job = initJobDto();

        when(jobProducer.produce(any()))
                .thenThrow(new JobAlreadyQueuedException(JOB_ID));

        // WHEN / THEN
        mockMvc.perform(MockMvcRequestBuilders.post(START_JOB_PATH)
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(job))
                )
                .andExpect(status().isConflict())
                .andExpect(content().string("\"job with id " + JOB_ID + " is already queued\""));

        ArgumentCaptor<JobDto> capturedJob = ArgumentCaptor.forClass(JobDto.class);
        verify(jobProducer, times(1))
                .produce(capturedJob.capture());

        assertEquals(1, capturedJob.getAllValues().size());
        checkJob(capturedJob.getValue());
    }

    @Test
    void getQueuedJobsIds() throws Exception {
        // GIVEN
        List<Long> queuedJobIds = new ArrayList<>();
        queuedJobIds.add(JOB_ID);
        queuedJobIds.add(JOB_ID + 5);
        queuedJobIds.add(JOB_ID - 3);

        when(jobProducer.getQueuedJobsIds())
                .thenReturn(queuedJobIds);

        // WHEN / THEN
        mockMvc.perform(get(QUEUED_JOBS_PATH))
                .andExpect(status().isOk())
                .andExpect(content().string(objectMapper.writeValueAsString(queuedJobIds)));

        verify(jobProducer, times(1)).getQueuedJobsIds();
    }

    @Test
    void removeJobFromQueue() throws Exception {
        // WHEN / THEN
        mockMvc.perform(delete(QUEUED_JOB_PATH, JOB_ID))
                .andExpect(status().isOk());

        verify(jobProducer, times(1)).removeJobFromQueue(JOB_ID);
    }

    @Test
    void clearJobQueue() throws Exception {
        // WHEN / THEN
        mockMvc.perform(delete(QUEUED_JOBS_PATH))
                .andExpect(status().isOk());

        verify(jobProducer, times(1)).clearJobQueue();
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
