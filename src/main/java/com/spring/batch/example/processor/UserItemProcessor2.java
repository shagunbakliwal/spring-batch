package com.spring.batch.example.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.spring.batch.example.domain.User;

@Component
public class UserItemProcessor2 implements ItemProcessor<User, User> {
	private static final Logger log = LoggerFactory.getLogger(UserItemProcessor2.class);

	@Override
	public User process(User user) throws Exception {
		// indicates that the item should not be continued to be processed
		if (user.getFirstName().equals("Shagun")) {
			return null;
		}
		final String firstName = user.getFirstName().toLowerCase();
		final String lastName = user.getLastName().toLowerCase();
		final int id = user.getId();
		final User transformedUser = new User(id, firstName, lastName);
		log.info("Converting (" + user + ") into (" + transformedUser + ")");
		return transformedUser;
	}

}
