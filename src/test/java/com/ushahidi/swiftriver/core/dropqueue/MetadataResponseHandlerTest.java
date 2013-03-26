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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

import com.rabbitmq.client.Channel;
import com.ushahidi.swiftriver.core.dropqueue.model.RawDrop;

public class MetadataResponseHandlerTest {
	
	private ObjectMapper objectMapper = new ObjectMapper();
	
	private Map<String, RawDrop> dropsMap;
	
	private BlockingQueue<RawDrop> publishQueue;
	
	private BlockingQueue<String> dropFilterQueue;

	private MetadataResponseHandler metadataResponseHandler;
	
	@Before
	public void setup() {
		dropsMap = new HashMap<String, RawDrop>();
		publishQueue = new LinkedBlockingQueue<RawDrop>();
		dropFilterQueue = new LinkedBlockingQueue<String>();
		
		metadataResponseHandler = new MetadataResponseHandler();
		metadataResponseHandler.setDropsMap(dropsMap);
		metadataResponseHandler.setObjectMapper(objectMapper);
		metadataResponseHandler.setPublishQueue(publishQueue);
		metadataResponseHandler.setDropFilterQueue(dropFilterQueue);
	}
	
	@Test
	public void onMediaExtractorMessage() throws Exception {
		Message mockMessage = mock(Message.class);
		MessageProperties mockMessageProperties = mock(MessageProperties.class);
		Channel mockChannel = mock(Channel.class);

		String body = "{\"source\":\"mediaextractor\",\"identity_orig_id\": \"http://feeds.bbci.co.uk/news/rss.xml\", \"droplet_raw\": \"The danger of growing resistance to antibiotics should be treated as seriously as the threat of terrorism, England's chief medical officer says.\", \"droplet_orig_id\": \"c558d88a44fc70da36d04746574e05e4\", \"droplet_locale\": \"en-gb\", \"identity_username\": \"http://www.bbc.co.uk/news/#sa-ns_mchannel=rss&ns_source=PublicRSS20-sa\", \"droplet_date_pub\": \"Mon, 11 Mar 2013 07:32:59 +0000\", \"droplet_type\": \"original\", \"identity_avatar\": \"http://news.bbcimg.co.uk/nol/shared/img/bbc_news_120x60.gif\", \"droplet_title\": \"Antibiotic resistance 'threat to UK'\", \"links\": [{\"url\": \"http://www.bbc.co.uk/news/health-21737844#sa-ns_mchannel=rss&ns_source=PublicRSS20-sa\", \"original_url\": true}], \"droplet_content\": \"The danger of growing resistance to antibiotics should be treated as seriously as the threat of terrorism, England's chief medical officer says.\", \"identity_name\": \"BBC News - Home\", \"channel\": \"rss\", \"river_id\": [2]}";
		when(mockMessage.getBody()).thenReturn(body.getBytes());
		when(mockMessage.getMessageProperties()).thenReturn(mockMessageProperties);
		when(mockMessageProperties.getCorrelationId()).thenReturn("correlation_id".getBytes());
		
		RawDrop rawDrop = new RawDrop(); 
		dropsMap.put("correlation_id", rawDrop);
		
		metadataResponseHandler.onMessage(mockMessage, mockChannel);
		
		assertTrue(rawDrop.isMediaComplete());
	}
	
	@Test
	public void onSemanticsMessage() throws Exception {
		Message mockMessage = mock(Message.class);
		MessageProperties mockMessageProperties = mock(MessageProperties.class);
		Channel mockChannel = mock(Channel.class);

		String body = "{\"source\":\"semantics\",\"identity_orig_id\": \"http://feeds.bbci.co.uk/news/rss.xml\", \"droplet_raw\": \"The danger of growing resistance to antibiotics should be treated as seriously as the threat of terrorism, England's chief medical officer says.\", \"droplet_orig_id\": \"c558d88a44fc70da36d04746574e05e4\", \"droplet_locale\": \"en-gb\", \"identity_username\": \"http://www.bbc.co.uk/news/#sa-ns_mchannel=rss&ns_source=PublicRSS20-sa\", \"droplet_date_pub\": \"Mon, 11 Mar 2013 07:32:59 +0000\", \"droplet_type\": \"original\", \"identity_avatar\": \"http://news.bbcimg.co.uk/nol/shared/img/bbc_news_120x60.gif\", \"droplet_title\": \"Antibiotic resistance 'threat to UK'\", \"links\": [{\"url\": \"http://www.bbc.co.uk/news/health-21737844#sa-ns_mchannel=rss&ns_source=PublicRSS20-sa\", \"original_url\": true}], \"droplet_content\": \"The danger of growing resistance to antibiotics should be treated as seriously as the threat of terrorism, England's chief medical officer says.\", \"identity_name\": \"BBC News - Home\", \"channel\": \"rss\", \"river_id\": [2]}";
		when(mockMessage.getBody()).thenReturn(body.getBytes());
		when(mockMessage.getMessageProperties()).thenReturn(mockMessageProperties);
		when(mockMessageProperties.getCorrelationId()).thenReturn("correlation_id".getBytes());
		
		RawDrop rawDrop = new RawDrop(); 
		dropsMap.put("correlation_id", rawDrop);
		
		metadataResponseHandler.onMessage(mockMessage, mockChannel);
		
		assertTrue(rawDrop.isSemanticsComplete());
	}
	
	@Test
	public void onRulesMessage() throws Exception {
		Message mockMessage = mock(Message.class);
		MessageProperties mockMessageProperties = mock(MessageProperties.class);
		Channel mockChannel = mock(Channel.class);

		String body = "{\"source\":\"rules\",\"identity_orig_id\": \"http://feeds.bbci.co.uk/news/rss.xml\", \"droplet_raw\": \"The danger of growing resistance to antibiotics should be treated as seriously as the threat of terrorism, England's chief medical officer says.\", \"droplet_orig_id\": \"c558d88a44fc70da36d04746574e05e4\", \"droplet_locale\": \"en-gb\", \"identity_username\": \"http://www.bbc.co.uk/news/#sa-ns_mchannel=rss&ns_source=PublicRSS20-sa\", \"droplet_date_pub\": \"Mon, 11 Mar 2013 07:32:59 +0000\", \"droplet_type\": \"original\", \"identity_avatar\": \"http://news.bbcimg.co.uk/nol/shared/img/bbc_news_120x60.gif\", \"droplet_title\": \"Antibiotic resistance 'threat to UK'\", \"links\": [{\"url\": \"http://www.bbc.co.uk/news/health-21737844#sa-ns_mchannel=rss&ns_source=PublicRSS20-sa\", \"original_url\": true}], \"droplet_content\": \"The danger of growing resistance to antibiotics should be treated as seriously as the threat of terrorism, England's chief medical officer says.\", \"identity_name\": \"BBC News - Home\", \"channel\": \"rss\", \"river_id\": [2]}";
		when(mockMessage.getBody()).thenReturn(body.getBytes());
		when(mockMessage.getMessageProperties()).thenReturn(mockMessageProperties);
		when(mockMessageProperties.getCorrelationId()).thenReturn("correlation_id".getBytes());
		
		RawDrop rawDrop = new RawDrop();
		dropsMap.put("correlation_id", rawDrop);
		
		metadataResponseHandler.onMessage(mockMessage, mockChannel);
		assertTrue(rawDrop.isRulesComplete());
	}
	
	@Test
	public void onMessage() throws Exception {
		Message mockMessage = mock(Message.class);
		MessageProperties mockMessageProperties = mock(MessageProperties.class);
		Channel mockChannel = mock(Channel.class);

		String body = "{\"source\":\"rules\",\"identity_orig_id\": \"http://feeds.bbci.co.uk/news/rss.xml\", \"droplet_raw\": \"The danger of growing resistance to antibiotics should be treated as seriously as the threat of terrorism, England's chief medical officer says.\", \"droplet_orig_id\": \"c558d88a44fc70da36d04746574e05e4\", \"droplet_locale\": \"en-gb\", \"identity_username\": \"http://www.bbc.co.uk/news/#sa-ns_mchannel=rss&ns_source=PublicRSS20-sa\", \"droplet_date_pub\": \"Mon, 11 Mar 2013 07:32:59 +0000\", \"droplet_type\": \"original\", \"identity_avatar\": \"http://news.bbcimg.co.uk/nol/shared/img/bbc_news_120x60.gif\", \"droplet_title\": \"Antibiotic resistance 'threat to UK'\", \"links\": [{\"url\": \"http://www.bbc.co.uk/news/health-21737844#sa-ns_mchannel=rss&ns_source=PublicRSS20-sa\", \"original_url\": true}], \"droplet_content\": \"The danger of growing resistance to antibiotics should be treated as seriously as the threat of terrorism, England's chief medical officer says.\", \"identity_name\": \"BBC News - Home\", \"channel\": \"rss\", \"river_id\": [2]}";
		when(mockMessage.getBody()).thenReturn(body.getBytes());
		when(mockMessage.getMessageProperties()).thenReturn(mockMessageProperties);
		when(mockMessageProperties.getCorrelationId()).thenReturn("correlation_id".getBytes());
		
		RawDrop rawDrop = new RawDrop();
		rawDrop.setSemanticsComplete(true);
		rawDrop.setMediaComplete(true);
		rawDrop.setRulesComplete(true);
		dropsMap.put("correlation_id", rawDrop);

		int size = dropFilterQueue.size();
		metadataResponseHandler.onMessage(mockMessage, mockChannel);
		assertTrue(dropsMap.isEmpty());
		assertTrue(publishQueue.contains(rawDrop));
		assertEquals(size + 1, dropFilterQueue.size());
	}
}
