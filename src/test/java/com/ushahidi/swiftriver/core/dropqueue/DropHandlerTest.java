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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.MessageProperties;

import com.rabbitmq.client.Channel;
import com.ushahidi.swiftriver.core.dropqueue.model.RawDrop;

public class DropHandlerTest {

	private ObjectMapper objectMapper = new ObjectMapper();

	private AmqpTemplate mockAmqpTemplate;

	private Map<String, RawDrop> dropsMap;

	private Queue mockCallbackQueue;

	private DropHandler dropHandler;

	@Before
	public void setup() {
		mockAmqpTemplate = mock(AmqpTemplate.class);
		dropsMap = new HashMap<String, RawDrop>();
		mockCallbackQueue = mock(Queue.class);
		
		dropHandler = new DropHandler();
		dropHandler.setAmqpTemplate(mockAmqpTemplate);
		dropHandler.setCallbackQueue(mockCallbackQueue);
		dropHandler.setDropsMap(dropsMap);
		dropHandler.setObjectMapper(objectMapper);
	}

	@Test
	public void onMessage() throws JsonParseException, JsonMappingException,
			IOException {
		Message mockMessage = mock(Message.class);
		MessageProperties mockMessageProperties = mock(MessageProperties.class);
		Channel mockChannel = mock(Channel.class);

		String body = "{\"identity_orig_id\": \"http://feeds.bbci.co.uk/news/rss.xml\", \"droplet_raw\": \"The danger of growing resistance to antibiotics should be treated as seriously as the threat of terrorism, England's chief medical officer says.\", \"droplet_orig_id\": \"c558d88a44fc70da36d04746574e05e4\", \"droplet_locale\": \"en-gb\", \"identity_username\": \"http://www.bbc.co.uk/news/#sa-ns_mchannel=rss&ns_source=PublicRSS20-sa\", \"droplet_date_pub\": \"Mon, 11 Mar 2013 07:32:59 +0000\", \"droplet_type\": \"original\", \"identity_avatar\": \"http://news.bbcimg.co.uk/nol/shared/img/bbc_news_120x60.gif\", \"droplet_title\": \"Antibiotic resistance 'threat to UK'\", \"links\": [{\"url\": \"http://www.bbc.co.uk/news/health-21737844#sa-ns_mchannel=rss&ns_source=PublicRSS20-sa\", \"original_url\": true}], \"droplet_content\": \"The danger of growing resistance to antibiotics should be treated as seriously as the threat of terrorism, England's chief medical officer says.\", \"identity_name\": \"BBC News - Home\", \"channel\": \"rss\", \"river_id\": [2]}";
		when(mockMessage.getBody()).thenReturn(body.getBytes());
		when(mockMessage.getMessageProperties()).thenReturn(mockMessageProperties);
		when(mockCallbackQueue.getName()).thenReturn("callback");
		
		dropHandler.onMessage(mockMessage, mockChannel);

		assertTrue(dropsMap.size() > 0);
		String correlationId = (String)dropsMap.keySet().toArray()[0];
		
		ArgumentCaptor<RawDrop> dropArgument = ArgumentCaptor
				.forClass(RawDrop.class);
		ArgumentCaptor<MessagePostProcessor> processorArgument = ArgumentCaptor
				.forClass(MessagePostProcessor.class);
		verify(mockAmqpTemplate).convertAndSend(dropArgument.capture(), processorArgument.capture());
		RawDrop drop = dropArgument.getValue();
		assertTrue(dropsMap.containsValue(drop));
		assertEquals("Antibiotic resistance 'threat to UK'", drop.getTitle());
		
		MessagePostProcessor postProcessor = processorArgument.getValue();
		postProcessor.postProcessMessage(mockMessage);
		verify(mockMessageProperties).setReplyTo("callback");
		verify(mockMessageProperties).setCorrelationId(correlationId.getBytes());
	}
}
