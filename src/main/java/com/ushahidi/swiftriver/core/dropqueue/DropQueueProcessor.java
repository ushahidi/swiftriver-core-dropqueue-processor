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
	final static Logger logger = LoggerFactory.getLogger(DropQueueProcessor.class);

	public static void main(String[] args) {
		AbstractApplicationContext context = new ClassPathXmlApplicationContext(
				"appContext.xml");
		context.registerShutdownHook();

		logger.info("Drop Queue Processor Started");
	}
}
