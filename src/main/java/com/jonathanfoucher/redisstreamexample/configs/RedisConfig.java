package com.jonathanfoucher.redisstreamexample.configs;

import com.jonathanfoucher.redisstreamexample.data.JobDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;

@Configuration
public class RedisConfig {
    @Autowired
    private StreamListener<String, ObjectRecord<String, JobDto>> streamListener;

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Value("${redis-stream-example.stream-key}")
    private String streamKey;
    @Value("${redis-stream-example.consumer-group-name}")
    private String groupName;

    @Bean
    public Subscription subscription(RedisConnectionFactory connectionFactory) throws UnknownHostException {
        createConsumerGroupIfNotExists(connectionFactory);

        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, ObjectRecord<String, JobDto>> options = StreamMessageListenerContainer
                .StreamMessageListenerContainerOptions
                .builder()
                .pollTimeout(Duration.ofMillis(100))
                .targetType(JobDto.class)
                .batchSize(1)
                .build();

        StreamMessageListenerContainer<String, ObjectRecord<String, JobDto>> container = StreamMessageListenerContainer
                .create(connectionFactory, options);

        Subscription subscription = container.receive(
                Consumer.from(groupName, InetAddress.getLocalHost().getHostName()),
                StreamOffset.create(streamKey, ReadOffset.lastConsumed()),
                streamListener
        );

        container.start();
        return subscription;
    }

    private void createConsumerGroupIfNotExists(RedisConnectionFactory connectionFactory) {
        try {
            connectionFactory.getConnection()
                    .streamCommands()
                    .xGroupCreate(streamKey.getBytes(), groupName, ReadOffset.latest(), true);
        } catch (RedisSystemException exception) {
            log.info("redis group might already exist, skipping redis group creation stream {} and group {}", streamKey, groupName);
        }
    }
}
