package com.book_master.batch.itemReaderAndWriter;

import org.assertj.core.api.Assertions;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.support.ClasspathXmlApplicationContextsFactoryBean;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.beans.PropertyVetoException;
import java.beans.beancontext.BeanContext;

@RunWith(value = SpringRunner.class)
@SpringBootTest(classes = {SavePersonConfiguration.class,TestConfiguration.class,SpringBatchTest.class})
public class SavePersonConfigurationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private  PersonRepository personRepository;


    @Test
    public  void test_step() {



        JobExecution jobExecution = jobLauncherTestUtils.launchStep("savePersonStep");

        int writerCnt=jobExecution.getStepExecutions().stream().mapToInt(StepExecution::getWriteCount).sum();

        Assertions.assertThat(writerCnt).isEqualTo(3);


    }


    @Test
    public void test_allow_duplicate() throws Exception {
        //given
        JobParameters jobParameters = new JobParametersBuilder().addString("allowDuplicate", "false")
                .toJobParameters();
        //when
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        //then
        int writerCnt=jobExecution.getStepExecutions().stream().mapToInt(StepExecution::getWriteCount).sum();

        System.out.println("writerCnt:::::::::::::"+writerCnt);
        Assertions.assertThat(writerCnt).isEqualTo(3).isEqualTo(personRepository.count());

        System.out.println(personRepository.findAll());

    }
}
