package com.book_master.batch.itemReaderAndWriter;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.security.PublicKey;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Configuration
@AllArgsConstructor
public class ItemProcessorConfiguration {
    private JobBuilderFactory jobBuilderFactory;
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job itemProcessorJob() {

        return jobBuilderFactory.get("itemProcessorJob")
                .incrementer(new RunIdIncrementer())
                .start(itemProcessorStep())
                .build();
    }


    @Bean
    public Step itemProcessorStep() {

        return stepBuilderFactory.get("itemProcessorStep")
                .<Person,Person>chunk(10)
                .reader(itemReader())
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();
    }

    private ItemWriter<? super Person> itemWriter() {

        return items -> {

            items.forEach(x->log.info("person id:{}",x.getId()));
        };
    }

    private ItemProcessor<? super Person,? extends Person> itemProcessor() {

        return item -> {

            if(item.getId()%2==0){

                return item;
            }
            return null;
        };

    }

    private ItemReader<? extends Person> itemReader() {
        return new CustomItemReader<>(getItems());
    }

    private List<Person> getItems() {

        List<Person> items = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            items.add(new Person(i+1,"test_name","test_age","test_address"));
        } 
        return items;
    }
}
