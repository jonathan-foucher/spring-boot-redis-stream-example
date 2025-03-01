## Introduction
This project is an example of Redis stream implementation with Spring Boot.

The objective is to create a queue for jobs to be processed one by one in the produced order.
If the app restart, it must process the job that was currently active and continue.
The app should also be able to return what jobs are currently queued and not allow to add an already queued job.

An endpoint allows to post a job in the queue and the consumer will automatically process it.
The producer and consumer are both set on the same project on this example.

## Run the project
You will need to launch a Redis instance on your computer before running the project.

You can either install Redis directly on your machine or run it through Docker :
`docker run -p 6379:6379 redis`

Once Redis is launched, you can start the Spring Boot project and start posting HTTP requests on the endpoint:
```
curl --request POST \
  --url http://localhost:8080/redis-stream-example/v1/jobs/start \
  --header 'Content-Type: application/json' \
  --data '{"id": 1, "name": "some job name"}'
```

There is also an endpoint to retrieve all queued jobs ids:
```
curl --request GET \
  --url http://localhost:8080/redis-stream-example/v1/jobs/queued
```

A custom health checker was added to check if the stream subscription is still active
It might be inactive when connection to redis is lost and won't recover the subscription
```
curl --request GET \
  --url http://localhost:8080/redis-stream-example/actuator/health
```
