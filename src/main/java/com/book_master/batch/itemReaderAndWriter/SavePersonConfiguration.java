package com.book_master.batch.itemReaderAndWriter;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.support.builder.CompositeItemProcessorBuilder;
import org.springframework.batch.item.support.builder.CompositeItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.persistence.EntityManagerFactory;
import java.security.PublicKey;

@Slf4j
@Configuration
@AllArgsConstructor
public class SavePersonConfiguration {
    private JobBuilderFactory jobBuilderFactory;
    private StepBuilderFactory stepBuilderFactory;

    private EntityManagerFactory entityManagerFactory;


    @Bean
    public Job savePersonJob() throws Exception {

        return jobBuilderFactory.get("savePersonJob")
                .incrementer(new RunIdIncrementer())
                .listener(new SavePersonListener.SavePersonJobExecutionListener())
                .listener(new SavePersonListener.SavePersonAnnotationJobExecutionListener())
                .start(savePersonStep(null))
                .build();
    }


    @JobScope
    public Step savePersonStep( @Value("#{jobParameters[allowDuplicate]}") String allowDuplicate ) throws Exception {

        return stepBuilderFactory.get("savePersonStep")
                .<Person,Person>chunk(10)
                .reader(itemReader())
                //.processor(new DuplicateValidationProcessor<>(Person::getName,Boolean.parseBoolean(allowDuplicate)))
                .processor(itemProcessor(allowDuplicate))
                .writer(itemWriter())
                .listener(new SavePersonListener.SavePersonAnnotationStepExecutionListener())
                .faultTolerant()
                .skip(NotFundNameException.class)
                .skipLimit(2)
                //.retry(NotFundNameException.class)
                //.retryLimit(3)
                .build();
    }

    private ItemProcessor<? super Person,? extends Person> itemProcessor(String allowDuplicate) throws Exception {

        DuplicateValidationProcessor<Person> duplicateValidationProcessor= new DuplicateValidationProcessor<Person>(Person::getName,Boolean.parseBoolean(allowDuplicate));

        ItemProcessor<Person,Person> validationProcessor = item ->{

         if(item.isNotEmptyName()) {
             return item;
         }
            throw new NotFundNameException();
        };

        CompositeItemProcessor<Person, Person> itemProcessor = new CompositeItemProcessorBuilder<Person,Person>().delegates(new PersonValidationRetryProcessor(), validationProcessor, duplicateValidationProcessor).build();

        itemProcessor.afterPropertiesSet();;

        return itemProcessor;
    }


    private ItemWriter<? super Person> itemWriter() throws Exception {

        //return items -> items.forEach(x->log.info("저는 {} 입니다",x.getName()));

        JpaItemWriter<Person> jpaItemWriter = new JpaItemWriterBuilder<Person>().entityManagerFactory(entityManagerFactory).build();

        ItemWriter<Person> logItemWriter = items -> log.info("person.size : {}",items.size());

        CompositeItemWriter<Person> compositeItemWriter = new CompositeItemWriterBuilder<Person>().delegates(jpaItemWriter, logItemWriter).build();

        compositeItemWriter.afterPropertiesSet();

        return compositeItemWriter;

    }

    private ItemReader<? extends Person> itemReader() throws Exception {

        DefaultLineMapper<Person> defaultLineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
        delimitedLineTokenizer.setNames("name","age","address");
        defaultLineMapper.setLineTokenizer(delimitedLineTokenizer);

        defaultLineMapper.setFieldSetMapper(fieldSet ->
                new Person(fieldSet.readString(0),fieldSet.readString(1),fieldSet.readString(2)));

        FlatFileItemReader<Person> itemReader = new FlatFileItemReaderBuilder<Person>()
                .name("savePersonItemReader")
                .encoding("UTF-8")
                .linesToSkip(1)
                .resource(new ClassPathResource("person.csv"))
                .lineMapper(defaultLineMapper)
                .build();

        itemReader.afterPropertiesSet();

        return itemReader;
    }


}
