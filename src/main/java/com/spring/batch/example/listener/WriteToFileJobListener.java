package com.spring.batch.example.listener;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.stereotype.Component;

@Component
public class WriteToFileJobListener extends JobExecutionListenerSupport {

	private static final Logger log = LoggerFactory.getLogger(WriteToFileJobListener.class);

	@Override
	public void afterJob(JobExecution jobExecution) {
		if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
			log.info("!!! JOB FINISHED! Time to verify the results in the file");
			try {
				RandomAccessFile randomAccessFile = new RandomAccessFile(new File("sample-data-output.csv"), "rw");
				while (randomAccessFile.getFilePointer() < randomAccessFile.length()) {
					log.info("Found :" + randomAccessFile.readLine());
				}
				randomAccessFile.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
