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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Drop Queue Processor
 * 
 * Takes drops posted onto the SwiftRiver DROPLET_QUEUE and posts them for
 * metadata extraction. Batches drops that have completed metadata extraction
 * for posting to the SwiftRiver REST API.
 * 
 */
public class DropQueueProcessor {

	final static Logger LOG = LoggerFactory.getLogger(DropQueueProcessor.class);

	public static void main(String[] args) {
		AbstractApplicationContext context = new ClassPathXmlApplicationContext(
				"appContext.xml");
		context.registerShutdownHook();
		
		DropFilterPublisher publisher = context.getBean(DropFilterPublisher.class);
		publisher.start();

		LOG.info("Drop Queue Processor Started");
	}
}
