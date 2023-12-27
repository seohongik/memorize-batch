package com.book_master.batch.tasklet;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Configuration
@AllArgsConstructor
public class JobConfig {

    private JobBuilderFactory jobBuilderFactory;
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job job(){
        return jobBuilderFactory.get("book_master_JobConfig")
                .incrementer(new RunIdIncrementer()) /*항상 다른 잡 인스턴스가 생성*/
                .start(startStep())
                .next(next01Step())
                .next(next02Step())
                .next(next03Step())
                .next(chunkStep(null))
                .next(taskletToChunkStep())
                .build();
    }

    @Bean
    public Step startStep(){
        return stepBuilderFactory.get("book_master_start_Step")
                .tasklet((contribution, chunkContext) -> {
                    log.info("!!!!!!!!!!!!!!!!!!hello !!!!!!!!!!!!!!!");

                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    public Step next01Step(){
        return stepBuilderFactory.get("book_master_next01_Step")
                .tasklet((contribution, chunkContext) -> {
                    StepExecution stepExecution = contribution.getStepExecution();
                    ExecutionContext stepExecutionContext = stepExecution.getExecutionContext();
                    stepExecutionContext.putString("stepKey", "step execution context");

                    JobExecution jobExecution = stepExecution.getJobExecution();
                    JobInstance jobInstance=jobExecution.getJobInstance();

                    ExecutionContext jobExecutionContext = jobExecution.getExecutionContext();
                    jobExecutionContext.putString("jobKey", "job execution context");
                    JobParameters jobParameters = jobExecution.getJobParameters();

                    log.info("jobName:{}, stepName:{}, parameter:{}", jobInstance.getJobName(), stepExecution.getStepName(), jobParameters.getLong("run.id"));

                    return RepeatStatus.FINISHED;
                }).build();
    }


    @Bean
    public Step next02Step(){
        return stepBuilderFactory.get("book_master_next02_Step")
                .tasklet((contribution, chunkContext) -> {
                    StepExecution stepExecution = contribution.getStepExecution();
                    ExecutionContext stepExecutionContext = stepExecution.getExecutionContext();

                    JobExecution jobExecution = stepExecution.getJobExecution();
                    ExecutionContext jobExecutionContext = jobExecution.getExecutionContext();

                    log.info("jobKey:{}, stepKey:{}",
                            jobExecutionContext.getString("jobKey","emptyJobKey")
                            ,stepExecutionContext.getString("stepKey", "emptyStepKey")
                            );

                    return RepeatStatus.FINISHED;
                }).build();
    }


    @Bean
    public Step next03Step(){
        return stepBuilderFactory.get("book_master_next03_TaskStep")
                .tasklet(tasklet())
                .build();

    }

    private Tasklet tasklet(){
        return ((contribution, chunkContext) -> {
            List<String> items = getItems();
            log.info("get Item ::::{}",items);
            return RepeatStatus.FINISHED;
        });

    }

    private List<String> getItems(){
        List<String> items = new ArrayList<>();
        for(int i=0; i<100; i++){
            items.add(i+"Hello");
        }
        return items;
    }

    @Bean
    @JobScope
    public Step chunkStep(@Value( "#{jobParameters[chunkSize]}") String chunkSize ){
        return stepBuilderFactory.get("book_master_next03_ChunkStep")
                //첫번째 reader 에서 읽고 반환되는 input  두번째 프로세스 거치고 난 output
                .<String,String>chunk(Strings.isNotEmpty(chunkSize) ?Integer.parseInt(chunkSize) :10)
                .reader(itemReader())
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();

    }

    public ItemReader<String> itemReader(){
        return new ListItemReader<>(getItems());
    }

    public ItemProcessor<String,String> itemProcessor(){
        //null 이면 writer로 넘어가지 못함
        return item -> item+", Spring Batch";
    }

    public ItemWriter<String> itemWriter(){
        return items -> log.info("chunk item :{}",items);
    }


    @Bean
    public Step taskletToChunkStep(){
        return stepBuilderFactory.get("book_master_next05_taskletToChunkStep")
                .tasklet(taskletToChunkTasklet(null))
                .build();
    }

    //청크 사이즈 pageable (tasklet) 에서

    @Bean
    @StepScope // 주석처리하면 Step 생명주기 때문에 에러가 남 밸류주입 안됨
    public Tasklet taskletToChunkTasklet(@Value( "#{jobParameters[chunkSize]}") String val ){
        List<String> items = getItems();
        return (contribution, chunkContext) -> {
            StepExecution stepExecution = contribution.getStepExecution();
            //JobParameters jobParameters = stepExecution.getJobParameters();
            //String val = jobParameters.getString("chunkSize", "10");
            int chunkSize= Strings.isNotEmpty(val) ?Integer.parseInt(val) :10;
            int fromIdx = stepExecution.getReadCount();
            int toIdx = fromIdx+chunkSize;

            if(fromIdx>=items.size()){
                return RepeatStatus.FINISHED;
            }

            List<String> subList = items.subList(fromIdx, toIdx);

            stepExecution.setReadCount(toIdx);
            log.info("taskToChunk Item ::::{}",subList.size());
            return RepeatStatus.CONTINUABLE;
        };

    }
}
