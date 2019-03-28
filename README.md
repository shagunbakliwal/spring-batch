# spring-batch
Spring Boot App using spring-batch for batching up jobs with mysql database .
BatchConfiguration which will
1) create datasource
2) read from file(FlatFileItemReader), write to db(JdbcBatchItemWriter)
3) read from db(JdbcCursorItemReader), write to file(FlatFileItemWriter)
4) create 2 jobs - readFromFileAndWriteToDb and readFromDatabaseAndWriteToFile which uses 2 individual steps
4) trigger listener on completion of job


