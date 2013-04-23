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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;

import com.ushahidi.swiftriver.core.dropqueue.model.RawDrop;

/**
 * Daemon that posts drops that have undergone semantic and metadata extraction
 * to the RULES_QUEUE for additional processing by the rules engine
 *  
 * @author ekala
 *
 */
public class DropFilterPublisher extends Thread {
	
	private BlockingQueue<String> dropFilterQueue;
	
	private ConcurrentMap<String, RawDrop> dropsMap;
	
	private AmqpTemplate amqpTemplate;
	
	private String callbackQueueName;
	
	final static Logger LOG = LoggerFactory.getLogger(DropFilterPublisher.class);
	
	public BlockingQueue<String> getDropFilterQueue() {
		return dropFilterQueue;
	}

	public void setDropFilterQueue(BlockingQueue<String> dropFilterQueue) {
		this.dropFilterQueue = dropFilterQueue;
	}

	public AmqpTemplate getAmqpTemplate() {
		return amqpTemplate;
	}

	public void setAmqpTemplate(AmqpTemplate amqpTemplate) {
		this.amqpTemplate = amqpTemplate;
	}

	public ConcurrentMap<String, RawDrop> getDropsMap() {
		return dropsMap;
	}

	public void setDropsMap(ConcurrentMap<String, RawDrop> dropsMap) {
		this.dropsMap = dropsMap;
	}

	public String getCallbackQueueName() {
		return callbackQueueName;
	}

	public void setCallbackQueueName(String callbackQueueName) {
		this.callbackQueueName = callbackQueueName;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	public void run() {
		LOG.debug("Rules publisher started");

		try {
			while (true) {
				publishDrop(dropFilterQueue.take());
			}
		} catch (InterruptedException e) {
			LOG.error(e.getMessage());
		}
	}
	
	public void publishDrop(String dropsMapKey) {
		LOG.debug(String.format("Sending drop with correlation id %s to rules processor",
				dropsMapKey));
		
		synchronized (dropsMap) {
			RawDrop drop  = dropsMap.get(dropsMapKey);

			// Drop doesn't exist; purge
			if (drop == null) {
				LOG.info("Drop with correlation ID '{}' not found", dropsMapKey);
				return;
			}

			final byte[] correlationId = dropsMapKey.getBytes();
			final String replyTo = this.getCallbackQueueName();

			amqpTemplate.convertAndSend(drop, new MessagePostProcessor() {
				public Message postProcessMessage(Message message) throws AmqpException {
					message.getMessageProperties().setCorrelationId(correlationId);
					message.getMessageProperties().setReplyTo(replyTo);
					return message;
				}
			});
		}
	}
	
}
