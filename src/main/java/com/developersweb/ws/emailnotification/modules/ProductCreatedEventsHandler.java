package com.developersweb.ws.emailnotification.modules;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import com.developer.ws.core.ProductCreatedEvent;
import com.developersweb.ws.emailnotification.errors.NotRetryableException;
import com.developersweb.ws.emailnotification.errors.RetryableExceptions;
import com.developersweb.ws.emailnotification.io.ProcessedEventEntity;
import com.developersweb.ws.emailnotification.io.ProcessedEventRepository;

import jakarta.transaction.Transactional;

@Component
@KafkaListener(topics =  "product-created-topic-event", groupId="product-created-events")
public class ProductCreatedEventsHandler {
	
	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
	@Autowired
	private RestTemplate restTemplate;
	@Autowired
	private ProcessedEventRepository processedEventsRepository;

//	public ProductCreatedEventsHandler(RestTemplate restTemplate, ProcessedEventRepository processedEventRepository) {
//		/* this.restTemplate = restTemplate; */
//		this.processedEventsRepository = processedEventsRepository;
//		
//	}
	
	
	
	
	public ProductCreatedEventsHandler()
	{
		
	}

	//Marking with transactional works as if it fails then all the operations performed on DB will be rolled  back
	@Transactional
	@KafkaHandler
	public void handle(@Payload ProductCreatedEvent productCreatedEvent,
			@Header(value="messageId",required=true) String messageId,
			@Header(KafkaHeaders.RECEIVED_KEY) String messageKey) {
		
		LOGGER.info("Received a new event: " + productCreatedEvent.getTitle());
		// Check duplicate message id 
		
		  Optional<ProcessedEventEntity> existingRecord =
		  Optional.ofNullable(processedEventsRepository.findByMessageId(messageId));
		  
		  if(existingRecord.isPresent()) {
		  LOGGER.error("Duplicate record found with same message id "+
		  existingRecord.get()); 
		  return;
		  }
		 
		

		String requestUrl = "http://localhost:8082/response/200";

		try {
			ResponseEntity<String> response = restTemplate.exchange(requestUrl, HttpMethod.GET, null, String.class);

			if (response.getStatusCode().value() == HttpStatus.OK.value()) {
				LOGGER.info("Received response from a remote service: " + response.getBody());
			}
		} catch (ResourceAccessException ex) {
			LOGGER.error(ex.getMessage());
			throw new RetryableExceptions(ex);
		} catch(HttpServerErrorException ex) {
			LOGGER.error(ex.getMessage());
			throw new NotRetryableException(ex);
		} catch(Exception ex) {
			LOGGER.error(ex.getMessage());
			throw new NotRetryableException(ex);
		}
		
		
		try {
		//Save unique id in database table
			processedEventsRepository.save(new ProcessedEventEntity(messageId,productCreatedEvent.getProductId()));
		}catch(DataIntegrityViolationException ex)
		{
			LOGGER.error("Error while saving data in DB"+ messageId + " "+ productCreatedEvent.getProductId());
			throw new NotRetryableException(ex);
		}

	}
	 

}
