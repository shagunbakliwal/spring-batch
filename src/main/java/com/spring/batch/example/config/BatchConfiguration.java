package com.spring.batch.example.config;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.spring.batch.example.domain.User;
import com.spring.batch.example.listener.WriteToDatabaseJobListener;
import com.spring.batch.example.listener.WriteToFileJobListener;
import com.spring.batch.example.processor.UserItemProcessor1;
import com.spring.batch.example.processor.UserItemProcessor2;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	UserItemProcessor1 userItemProcessor1;
	@Autowired
	UserItemProcessor2 userItemProcessor2;

	@Autowired
	public DataSource dataSource;

	// create datasource with mysql
	@Bean
	public DataSource dataSource() {
		final DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
		dataSource.setUrl("jdbc:mysql://localhost:3306/springbatch?useSSL=false");
		dataSource.setUsername("root");
		dataSource.setPassword("root");
		return dataSource;
	}

	// [job1] read from file
	@Bean
	public FlatFileItemReader<User> readFromFile() {
		return new FlatFileItemReaderBuilder<User>().name("userItemReader")
				.resource(new FileSystemResource("sample-data.csv")).delimited()
				.names(new String[] { "id", "firstName", "lastName" })
				.fieldSetMapper(new BeanWrapperFieldSetMapper<User>() {
					{
						setTargetType(User.class);
					}
				}).build();
	}

	// [job1] write to database
	@Bean
	public JdbcBatchItemWriter<User> writerToDatabase() {
		return new JdbcBatchItemWriterBuilder<User>()
				.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
				.sql("INSERT INTO user (id,first_name,last_name) VALUES (:id,:firstName,:lastName)")
				.dataSource(dataSource).build();
	}

	// [job2] read from database
	@Bean("readFromDatabase")
	public JdbcCursorItemReader<User> readFromDatabase() {
		JdbcCursorItemReader<User> reader = new JdbcCursorItemReader<User>();
		reader.setDataSource(dataSource);
		reader.setSql("SELECT id,first_name,last_name FROM user");
		reader.setRowMapper(new RowMapper<User>() {
			@Override
			public User mapRow(ResultSet rs, int rowNum) throws SQLException {
				User user = new User();
				user.setId(rs.getInt("id"));
				user.setFirstName(rs.getString("first_name"));
				user.setLastName(rs.getString("last_name"));
				return user;
			}
		});
		return reader;
	}

	// [job2] write to file
	@Bean("writerToFile")
	public FlatFileItemWriter<User> writerToFile() {
		FlatFileItemWriter<User> writer = new FlatFileItemWriter<>();
		writer.setResource(new FileSystemResource("sample-data-output.csv"));
		writer.setLineAggregator(new DelimitedLineAggregator<User>() {
			{
				setDelimiter(",");
				setFieldExtractor(new BeanWrapperFieldExtractor<User>() {
					{
						setNames(new String[] { "id", "firstName", "lastName" });
					}
				});
			}
		});
		writer.setAppendAllowed(true);
		return writer;
	}

	// job1 readFromFileAndWriteToDb
	@Bean("readFromFileAndWriteToDb")
	public Job readFromFileAndWriteToDb(WriteToDatabaseJobListener writeToDatabaseJobListener,
			@Qualifier("step1") Step step1) {
		return jobBuilderFactory.get("readFromFileAndWriteToDb").incrementer(new RunIdIncrementer())
				.listener(writeToDatabaseJobListener).flow(step1).end().build();
	}

	// job2 readFromDatabaseAndWriteToFile
	@Bean("readFromDatabaseAndWriteToFile")
	public Job readFromDatabaseAndWriteToFile(WriteToFileJobListener writeToFileJobListener,
			@Qualifier("step2") Step step2) {
		return jobBuilderFactory.get("readFromDatabaseAndWriteToFile").incrementer(new RunIdIncrementer())
				.listener(writeToFileJobListener).flow(step2).end().build();
	}

	@Bean(name = "step1")
	public Step step1(JdbcBatchItemWriter<User> writeToDatabase) {
		return stepBuilderFactory.get("step1").<User, User>chunk(10).reader(readFromFile())
				.processor(userItemProcessor1).writer(writeToDatabase).build();
	}

	@Bean(name = "step2")
	public Step step2(@Qualifier("writerToFile") FlatFileItemWriter<User> writeToFile) {
		return stepBuilderFactory.get("step2").<User, User>chunk(10).reader(readFromDatabase())
				.processor(userItemProcessor2).writer(writeToFile).build();
	}

}
