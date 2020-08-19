package lt.bauzys.sbatch.csvrest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
    private static final int TASK_COUNT = 20;

    private static final AtomicInteger taskCount = new AtomicInteger(0);
    private static final Logger log = LoggerFactory.getLogger(BatchConfiguration.class);

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    public BatchConfiguration(JobBuilderFactory jobBuilderFactory,
                              StepBuilderFactory stepBuilderFactory
    ) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    protected Job job(@Qualifier("step1") Step step1) {
        return jobBuilderFactory
                .get("myJob")
                .start(step1)
                .build();
    }

    @Bean
    protected WebClient webClient() {
        return WebClient.create("http://localhost:8888");
    }

    @Bean
    public Tasklet requestQuoteTask(WebClient webClient) {
        return (stepContribution, chunkContext) -> {
            for (int i = 0; i < TASK_COUNT; i++) {
                Mono<Quote> quote = webClient.get()
                        .uri("/api/quote/random")
                        .retrieve()
                        .onStatus(HttpStatus::is5xxServerError, response -> Mono.just(new Exception("500 Error!")))
                        .bodyToMono(Quote.class);

                quote.subscribe( q -> log.info("task: {} - {}", taskCount.incrementAndGet(), q.toString()));
            }
            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    protected Step step1(Tasklet tasklet, TaskExecutor taskExecutor) {
        return stepBuilderFactory
                .get("step1")
                .tasklet(tasklet)
                .taskExecutor(taskExecutor)
                .allowStartIfComplete(true)
                .build();
    }
}
