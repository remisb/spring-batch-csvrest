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
//import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.boot.web.reactive.server.ReactiveWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
//import org.springframework.web.client.RestTemplate;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import javax.sql.DataSource;
import java.util.concurrent.atomic.AtomicInteger;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
    private static final int TASK_COUNT = 20;

    private static AtomicInteger taskCount = new AtomicInteger(0);
    private static final Logger log = LoggerFactory.getLogger(BatchConfiguration.class);

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
//    private final RestTemplateBuilder restTemplateBuilder;
    private final DataSource dataSource;
    private final ReactiveWebServerFactory webServerFactory;

    public BatchConfiguration(JobBuilderFactory jobBuilderFactory,
                              StepBuilderFactory stepBuilderFactory,
                              DataSource dataSource,
//                              RestTemplateBuilder restTemplateBuilder
                              ReactiveWebServerFactory webServerFactory
    ) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
//        this.restTemplateBuilder = restTemplateBuilder;
        this.dataSource = dataSource;
        this.webServerFactory = webServerFactory;
    }

//    @Bean
//    protected RestTemplate restTemplate() {
//        return restTemplateBuilder.build();
//    }

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

//    @Bean
//    public Tasklet requestQuoteTask(RestTemplate restTemplate) {
//        return (stepContribution, chunkContext) -> {
//            for (int i = 0; i < TASK_COUNT; i++) {
//                Quote quote = restTemplate.getForObject(
//                        "http://localhost:8888/api/quote/random",
//                        Quote.class);
//                log.info("task: {} - {}", taskCount.incrementAndGet(), quote.toString());
//            }
//            return RepeatStatus.FINISHED;
//        };
//    }

    @Bean
    public Tasklet requestQuoteTask(WebClient webClient) {
        return (stepContribution, chunkContext) -> {
            for (int i = 0; i < TASK_COUNT; i++) {
                Mono<Quote> quote = webClient.get()
                        .uri("/api/quote/random")
                        .retrieve()
                        .bodyToMono(Quote.class);

                quote.subscribe(System.out::println);
//                log.info("task: {} - {}", taskCount.incrementAndGet(), quote.toString());
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
