package com.developersweb.ws.emailnotification.modules;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.stereotype.Component;

import com.developer.ws.core.ProductCreatedEvent;

@Component


public class ProductCreatedEventsHandler {
	
	public final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(getClass());
	
	@RetryableTopic(attempts = "3", dltStrategy = DltStrategy.FAIL_ON_ERROR)
	@KafkaListener(topics =  "product-created-topic-event", groupId="product-created-events")
	public void handleEvents(ProductCreatedEvent productCreatedEvent)
	{
		LOGGER.info("New product is created " + productCreatedEvent.getTitle());
	}
	
	@KafkaListener(topics="product-created-topic-event-dlt")
	public void listenToDLTErrors(ConsumerRecord<?, ?> record)
	{
		System.err.println("DLT received "+record.value());
		//System.err.println("Headers "+ record.headers());
		
	}

}
