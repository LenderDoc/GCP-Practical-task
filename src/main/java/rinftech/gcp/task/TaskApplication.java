package rinftech.gcp.task;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@SpringBootApplication
@EnableAsync
public class TaskApplication {

    @Value("${bigquery.executor.pool.size}")
    private int bigQueryThreadPoolSize;  
    
    @Value("${bigquery.executor.queue.capacity}")
    private int bigQueryThreadQueueCapacity;
        
    public static void main(String[] args) {
        SpringApplication.run(TaskApplication.class, args);
    }

    @Bean("bigQueryExecutor")
    public TaskExecutor bigQueryExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(bigQueryThreadPoolSize);
        executor.setMaxPoolSize(bigQueryThreadPoolSize);
        executor.setQueueCapacity(bigQueryThreadQueueCapacity);
        executor.setThreadNamePrefix("bigQuery-");
        executor.initialize();
        return executor;
    }
}
