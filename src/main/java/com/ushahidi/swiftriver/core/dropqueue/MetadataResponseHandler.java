/**
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.ushahidi.swiftriver.core.dropqueue;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.util.ErrorHandler;

import com.rabbitmq.client.Channel;
import com.ushahidi.swiftriver.core.dropqueue.model.RawDrop;

/**
 * Handler for incoming drops from metadata extractors.
 * 
 * Update the in memory drops map with the response from the metadata
 * extractors.
 * 
 * Puts drops that have completed metadata extraction onto a publish queue for
 * posting to the SwiftRiver REST API.
 * 
 */
public class MetadataResponseHandler implements ChannelAwareMessageListener,
		ErrorHandler {

	final Logger logger = LoggerFactory
			.getLogger(MetadataResponseHandler.class);

	private ObjectMapper objectMapper;

	private Map<String, RawDrop> dropsMap;

	private BlockingQueue<RawDrop> publishQueue;
	
	private BlockingQueue<String> dropFilterQueue;
	
	private Map<String, DeliveryFrame> deliveryFramesMap;
	
	public void setObjectMapper(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	public void setDropsMap(Map<String, RawDrop> dropsMap) {
		this.dropsMap = dropsMap;
	}

	public void setPublishQueue(BlockingQueue<RawDrop> publishQueue) {
		this.publishQueue = publishQueue;
	}

	public void setDropFilterQueue(BlockingQueue<String> dropFilterQueue) {
		this.dropFilterQueue = dropFilterQueue;
	}

	public void setDeliveryFramesMap(Map<String, DeliveryFrame> deliveryFramesMap) {
		this.deliveryFramesMap = deliveryFramesMap;
	}

	/**
	 * Receive drop that has completed metadata extraction.
	 * 
	 * Updates the locally cached drop with received metadata. 
	 * Drops that have completed both media and sematic extraction
	 * get added to the publishQueue for posting to the API.
	 * 
	 * @param message
	 * @param channel
	 * @throws Exception
	 */
	public void onMessage(Message message, Channel channel) throws Exception {
		String correlationId = new String(message.getMessageProperties()
				.getCorrelationId());
		RawDrop updatedDrop = objectMapper.readValue(
				new String(message.getBody()), RawDrop.class);

		logger.info("Metadata Response received from '{}' with correlation_id '{}'",
						updatedDrop.getSource(), correlationId);

		synchronized (dropsMap) {
			RawDrop cachedDrop = dropsMap.get(correlationId);

			// Verify that the drop exists in the in-memory cache
			if (cachedDrop == null) {
				logger.error("Drop with correlation id '{}' not found in cache",
						correlationId);

				// Acknowledge receipt
				channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
				return;
			}

			if (updatedDrop.getSource().equals("mediaextractor")) {
				cachedDrop.setMediaComplete(true);
				cachedDrop.setMedia(updatedDrop.getMedia());
				cachedDrop.setLinks(updatedDrop.getLinks());
			} else if (updatedDrop.getSource().equals("semantics")) {
				cachedDrop.setSemanticsComplete(true);
				cachedDrop.setTags(updatedDrop.getTags());
				cachedDrop.setPlaces(updatedDrop.getPlaces());
			} else if (updatedDrop.getSource().equals("rules")) {
				cachedDrop.setBucketIds(updatedDrop.getBucketIds());
				cachedDrop.setRiverIds(updatedDrop.getRiverIds());
				cachedDrop.setMarkAsRead(updatedDrop.getMarkAsRead());
				cachedDrop.setRulesComplete(true);
			}

			// When semantics and metadata extraction are complete,
			// submit for rules processing
			if (cachedDrop.isSemanticsComplete() && cachedDrop.isMediaComplete()) {
				logger.info("Sending drop with correlation id '{}' for rules processing",
						correlationId);
				dropFilterQueue.put(correlationId);
			}

			if (cachedDrop.isSemanticsComplete()
					&& cachedDrop.isMediaComplete() && cachedDrop.isRulesComplete()) {

				// Queue the drop for posting via the API
				if (cachedDrop.getRiverIds() != null && 
						!cachedDrop.getRiverIds().isEmpty()) {
					publishQueue.put(cachedDrop);
				} else {
					logger.info("No destination rivers for drop with correlation id '{}'",
							correlationId);
				}

				dropsMap.remove(correlationId);

				// Confirm the drop has completed processing
				DeliveryFrame deliveryFrame = deliveryFramesMap.remove(correlationId);
				Channel confirmChannel = deliveryFrame.getChannel();
				confirmChannel.basicAck(deliveryFrame.getDeliveryTag(), false);

				// Log
				logger.info("Drop with correlation id '{}' has completed metadata extraction",
						correlationId);
			}
		}
	}

	public void handleError(Throwable t) {
		logger.error("Metadata response error", t);
	}

}
