package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.FieldExtractor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.WritableResource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {
    private static final Logger logger = LoggerFactory.getLogger(SpringBatchConfig.class);

    @Value("${output.file}")
    private String outputFile;

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final DataSource dataSource;

    public SpringBatchConfig(
        final JobRepository jobRepository,
        final PlatformTransactionManager transactionManager,
        DataSource dataSource) {
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.dataSource = dataSource;
    }

    @Bean
    public Job batchJob(Step batchStep) {
        return new JobBuilder("batchJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(batchStep)
                .build();
    }

    @Bean
    public Step batchStep(
            ItemReader<PersonCsv> reader,
            ItemProcessor<PersonCsv, PersonCsvOut> processor,
            ItemWriter<PersonCsvOut> writer
    ) {
        return new StepBuilder("batchStep", jobRepository)
                .<PersonCsv, PersonCsvOut>chunk(3, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<PersonCsv> reader(
            @Value("${input.file}") FileSystemResource fileSystemResource
    ) {
        return new FlatFileItemReaderBuilder<PersonCsv>()
            .name("personItemReader")
            .resource(fileSystemResource)
            .linesToSkip(1)
            .delimited()
            .names("person_ID", "name", "first", "last", "middle", "email", "phone", "fax", "title")
            .targetType(PersonCsv.class)
            .build();
    }

    @Bean
    public ItemProcessor<PersonCsv, PersonCsvOut> processor() {
        return personCsv -> {
            if (personCsv.title().contains("Professor")) {
                return null;
            }
            return new PersonCsvOut(personCsv.first(), personCsv.last());
        };
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<PersonCsvOut> writer(@Value("#{stepExecutionContext['dynamicFileName']}") String dynamicFileName) {
        if (dynamicFileName == null) {
            throw new IllegalArgumentException("O caminho do arquivo dinâmico não pode ser nulo.");
        }

        WritableResource dynamicOutputFile = new FileSystemResource(dynamicFileName);

        return  new FlatFileItemWriterBuilder<PersonCsvOut>()
                .name("itemWriter")
                .resource(dynamicOutputFile)
                .delimited()
                .delimiter(",")
                .fieldExtractor(fieldExtractor())
                .build();

    }

    @Bean
    public StepExecutionListener stepExecutionListener() {
        return new StepExecutionListener() {
            @Override
            public void beforeStep(StepExecution stepExecution) {
                // Concatena o valor da propriedade com o timestamp
                String dynamicFileName = outputFile + "_" + System.currentTimeMillis() + ".csv";

                // Armazena o nome do arquivo dinâmico no contexto do passo
                stepExecution.getExecutionContext().put("dynamicFileName", dynamicFileName);
            }

            @Override
            public ExitStatus afterStep(StepExecution stepExecution) {
                // Lógica após a conclusão do Step, se necessário
                return null;
            }
        };
    }

    @Bean
    public FieldExtractor<PersonCsvOut> fieldExtractor() {
        BeanWrapperFieldExtractor<PersonCsvOut> extractor = new BeanWrapperFieldExtractor<>();
        extractor.setNames(new String[]{"first", "last"});
        return extractor;
    }

}