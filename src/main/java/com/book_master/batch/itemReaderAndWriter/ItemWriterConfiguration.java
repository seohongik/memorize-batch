package com.book_master.batch.itemReaderAndWriter;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Configuration
@AllArgsConstructor
public class ItemWriterConfiguration {
    private JobBuilderFactory jobBuilderFactory;
    private StepBuilderFactory stepBuilderFactory;
    private DataSource dataSource;
    private EntityManagerFactory entityManagerFactory;
    @Bean
    public Job itemWriterJob() throws Exception {

        return jobBuilderFactory.get("itemWriterJob")
                .incrementer(new RunIdIncrementer())
                .start(csvItemWriterStep())
                //.next(jdbcBatchItemStep())
                .next(jpaItemWriterStep())
                .build();
    }


    @Bean
    public Step csvItemWriterStep() throws Exception {

        return stepBuilderFactory.get("csvItemWriterStep")
                .<Person,Person>chunk(10)
                .reader(itemReader())
                //.processor()
                .writer(csvFileItemWriter())
                .writer(jdbcBatchItemWriter())
                .build();
    }

    private ItemWriter<? super Person> csvFileItemWriter() throws Exception {

        BeanWrapperFieldExtractor<Person> fieldExtract = new BeanWrapperFieldExtractor<>();
        fieldExtract.setNames(new String[]{"id","name","age","address"});
        DelimitedLineAggregator<Person> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        lineAggregator.setFieldExtractor(fieldExtract);

        FlatFileItemWriter<Person> itemWriter = new FlatFileItemWriterBuilder<Person>()
                .name("csvFileItemWriter")
                .encoding("UTF-8")
                .resource(new FileSystemResource("out/test-output.csv"))
                .lineAggregator(lineAggregator)
                .headerCallback(writer -> writer.write("id,이름,나이,거주지"))
                .footerCallback(writer -> writer.write("-----------------\r"))
                .append(true)
                .build();

        itemWriter.afterPropertiesSet();;

        return itemWriter;
    }

    private ItemReader<Person> itemReader() {

        return  new CustomItemReader<>(getItems());
    }

    private List<Person> getItems(){

        List<Person> items = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            //items.add(new Person(i+1,"test name","test age","test address")); merge
            items.add(new Person("test name","test age","test address")); //not merge
        }

        return items;

    }

    @Bean
    public Step jdbcBatchItemStep(){

        return stepBuilderFactory.get("jdbcBatchItemStep")
                .<Person,Person>chunk(10)
                .reader(itemReader())
                .writer(jdbcBatchItemWriter())
                .build();
    }

    private ItemWriter<? super Person> jdbcBatchItemWriter() {

        JdbcBatchItemWriter<Person> itemWriter = new JdbcBatchItemWriterBuilder<Person>()
                .dataSource(dataSource)
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("insert into person ( name , age , address) values(:name, :age, :address) ")
                .build();

        itemWriter.afterPropertiesSet();;

        return itemWriter;
    }

    @Bean Step jpaItemWriterStep() throws Exception {

        return stepBuilderFactory.get("jpaItemWriterStep")
                .<Person,Person>chunk(10)
                .reader(itemReader())
                .writer(jpaItemWriter())
                .build();
    }

    private ItemWriter<? super Person> jpaItemWriter() throws Exception {

        JpaItemWriter<Person> itemWriter = new JpaItemWriterBuilder<Person>()
                .entityManagerFactory(entityManagerFactory)
                .usePersist(true) // defualt merge usePersist true => not merge
                .build();

        itemWriter.afterPropertiesSet();

        return itemWriter;
    }

}
