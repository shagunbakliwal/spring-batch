
package com.spring.batch.example.listener;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.spring.batch.example.domain.User;

@Component
public class WriteToDatabaseJobListener extends JobExecutionListenerSupport {

	private static final Logger log = LoggerFactory.getLogger(WriteToDatabaseJobListener.class);

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Override
	public void afterJob(JobExecution jobExecution) {
		if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
			log.info("!!! JOB FINISHED! Time to verify the results in the database");
			jdbcTemplate.query("SELECT id,first_name,last_name FROM user", new RowMapper<User>() {

				@Override
				public User mapRow(ResultSet resultSet, int rowNum) throws SQLException {
					return new User(resultSet.getInt("id"), resultSet.getString("first_name"),
							resultSet.getString("last_name"));
				}

			}).forEach(person -> log.info("Found <" + person + "> in the database."));
		}
	}
}
