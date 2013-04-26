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

import org.springframework.amqp.core.Message;

import com.rabbitmq.client.Channel;

/**
 * This class is an abstraction of a message delivery frame. The
 * frame is comprised of a delivery tag of type <code>long</code>
 * associated with a given {@link Message} and the {@link Channel}
 * used to deliver that message
 * 
 * @author ekala
 *
 */
public class DeliveryFrame {

	/** Delivery Tag */
	private long deliveryTag;

	/** Channel used to delivery the message */
	private Channel channel;

	public DeliveryFrame() {
	}

	/**
	 * Creates and sets the delivery tag and channel
	 *  
	 * @param deliveryTag
	 * @param channel
	 */
	public DeliveryFrame(long deliveryTag, Channel channel) {
		this.deliveryTag = deliveryTag;
		this.channel = channel;
	}

	public long getDeliveryTag() {
		return deliveryTag;
	}

	public void setDeliveryTag(long deliveryTag) {
		this.deliveryTag = deliveryTag;
	}

	public Channel getChannel() {
		return channel;
	}

	public void setChannel(Channel channel) {
		this.channel = channel;
	}
}
