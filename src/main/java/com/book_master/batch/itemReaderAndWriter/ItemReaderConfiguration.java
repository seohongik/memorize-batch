package com.book_master.batch.itemReaderAndWriter;

import com.book_master.batch.domain.Person;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.JpaCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JpaCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Configuration
@AllArgsConstructor
public class ItemReaderConfiguration {
    private JobBuilderFactory jobBuilderFactory;
    private StepBuilderFactory stepBuilderFactory;
    private DataSource dataSource;
    private EntityManagerFactory entityManagerFactory;

    @Bean
    public Job itemReaderJob() throws Exception {
        return jobBuilderFactory.get("itemReaderJob")
                .incrementer(new RunIdIncrementer())
                .start(customItemReaderStep())
                .next(csvFileStep())
                .next(jdbcStep())
                .next(jpaStep())
                .build();
    }

    @Bean
    public Step csvFileStep() throws Exception {
        return stepBuilderFactory.get("csvFileStep")
                .<Person, Person>chunk(10)
                .reader(csvFileItemReader())
                .writer(itemWriter())
                .build();
    }

    private JdbcCursorItemReader<Person> jdbcCursorItemReader() throws Exception {
        JdbcCursorItemReader<Person> itemReader = new JdbcCursorItemReaderBuilder<Person>()
                .name("jdbcCursorItemReader")
                .dataSource(dataSource)
                .sql("select id, name, age, address from person")
                .rowMapper((rs, roNum) -> new Person(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getString(4))
                ).build();

        itemReader.afterPropertiesSet();
        return itemReader;
    }

    @Bean
    public Step jdbcStep() throws Exception {
        return stepBuilderFactory.get("jdbcStep")
                .<Person, Person>chunk(10)
                .reader(jdbcCursorItemReader())
                .writer(itemWriter())
                .build();
    }

    private FlatFileItemReader<Person> csvFileItemReader() throws Exception {
        DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();

        tokenizer.setNames("id", "name", "age", "address");
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(fieldSet -> {
            int id = fieldSet.readInt("id");
            String name = fieldSet.readString("name");
            String age = fieldSet.readString("age");
            String address = fieldSet.readString("address");

            return new Person(id, name, age, address);
        });
        FlatFileItemReader<Person> itemReader = new FlatFileItemReaderBuilder<Person>()
                .name("csvFileItemReader")
                .encoding("UTF-8")
                .resource(new ClassPathResource("test.csv"))
                .linesToSkip(1)
                .lineMapper(lineMapper)
                .build();

        itemReader.afterPropertiesSet();//설정값이 제대로 설정되었는기 검증
        return itemReader;
    }


    @Bean
    public Step customItemReaderStep() {
        return stepBuilderFactory.get("customItemReaderStep")
                .<Person, Person>chunk(10)
                .reader(new CustomItemReader<>(getItems()))
                //.processor()
                .writer(itemWriter())
                .build();
    }

    private ItemWriter<? super Person> itemWriter() {
        return items -> log.info(items.stream().map(Person::getName).collect(Collectors.joining(",")));
    }


    private List<Person> getItems() {
        List<Person> items = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            items.add(new Person(i + 1, "test name" + i, "test_age", "test_address"));
        }
        return items;
    }

    @Bean
    public Step jpaStep() throws Exception {
        return stepBuilderFactory
                .get("jpaStep")
                .<Person, Person>chunk(10)
                .reader(jpaCursorItemReader())
                .writer(itemWriter())
                .build();
    }

    private JpaCursorItemReader<Person> jpaCursorItemReader() throws Exception {
        JpaCursorItemReader<Person> itemReader = new JpaCursorItemReaderBuilder<Person>()
                .name("jpaCursorItemReader")
                .entityManagerFactory(entityManagerFactory)
                .queryString("select p.name from Person p")
                .build();
        itemReader.afterPropertiesSet();
        return itemReader;

    }

}
