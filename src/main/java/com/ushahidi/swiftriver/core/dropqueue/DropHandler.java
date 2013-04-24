/**
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/agpl.html>
 * 
 * Copyright (C) Ushahidi Inc. All Rights Reserved.
 */
package com.ushahidi.swiftriver.core.dropqueue;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.UUID;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.util.ErrorHandler;

import com.rabbitmq.client.Channel;
import com.ushahidi.swiftriver.core.dropqueue.model.RawDrop;

/**
 * Handler for incoming drops on the Drop Queue
 * 
 * Assigns incoming drops a correlation ID and places them in an in memory up
 * before publishing them for meta-data extraction.
 * 
 */
public class DropHandler implements ChannelAwareMessageListener, ErrorHandler {

	final Logger logger = LoggerFactory.getLogger(DropQueueProcessor.class);

	private ObjectMapper objectMapper;

	private AmqpTemplate amqpTemplate;

	private Map<String, RawDrop> dropsMap;

	private Queue callbackQueue;
	
	private Map<String, Long> deliveryTagsMap;

	public ObjectMapper getObjectMapper() {
		return objectMapper;
	}

	public void setObjectMapper(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	public AmqpTemplate getAmqpTemplate() {
		return amqpTemplate;
	}

	public void setAmqpTemplate(AmqpTemplate amqpTemplate) {
		this.amqpTemplate = amqpTemplate;
	}

	public Map<String, RawDrop> getDropsMap() {
		return dropsMap;
	}

	public void setDropsMap(Map<String, RawDrop> dropsMap) {
		this.dropsMap = dropsMap;
	}

	public Queue getCallbackQueue() {
		return callbackQueue;
	}

	public void setCallbackQueue(Queue callbackQueue) {
		this.callbackQueue = callbackQueue;
	}

	public void setDeliveryTagsMap(Map<String, Long> deliveryTagsMap) {
		this.deliveryTagsMap = deliveryTagsMap;
	}

	/**
	 * Receive drops placed on the DROPLET_QUEUE by channel apps.
	 * 
	 * Caches the drop in the dropsMap for metadata updates and then publishes
	 * the drop to the metadata exchange for metadata extraction to be
	 * performed.
	 * 
	 * @param message
	 * @throws IOException
	 * @throws JsonMappingException
	 * @throws JsonParseException
	 */
	public synchronized void onMessage(Message message, Channel channel)
			throws JsonParseException, JsonMappingException, IOException {

		RawDrop drop = objectMapper.readValue(new String(message.getBody()),
				RawDrop.class);

		final String correlationId = UUID.randomUUID().toString();
		final String replyTo = callbackQueue.getName();
		long deliveryTag = message.getMessageProperties().getDeliveryTag();
		
		dropsMap.put(correlationId, drop);		
		deliveryTagsMap.put(correlationId, Long.valueOf(deliveryTag));
		
		logger.debug("Sending drop with correlation ID {} to {}", correlationId, replyTo);
		amqpTemplate.convertAndSend(drop, new MessagePostProcessor() {
			public Message postProcessMessage(Message message)
					throws AmqpException {
				message.getMessageProperties().setReplyTo(replyTo);
				try {
					message.getMessageProperties().setCorrelationId(
							correlationId.getBytes("UTF-8"));
				} catch (UnsupportedEncodingException e) {
					throw new AmqpException(e);
				}
				return message;
			}
		});

		logger.debug("Drop sent for metadata extraction with correlation id '{}'",
				correlationId);
	}

	public void handleError(Throwable t) {
		logger.error("Error processing drop", t);
	}

}
