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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ushahidi.swiftriver.core.api.client.SwiftRiverClient;
import com.ushahidi.swiftriver.core.dropqueue.model.RawDrop;
import com.ushahidi.swiftriver.core.api.client.model.Drop;

/**
 * Publisher for drops that have completed metadata extraction.
 * 
 */
public class Publisher {

	final Logger logger = LoggerFactory.getLogger(Publisher.class);

	private BlockingQueue<RawDrop> publishQueue;

	private SwiftRiverClient apiClient;

	public BlockingQueue<RawDrop> getPublishQueue() {
		return publishQueue;
	}

	public void setPublishQueue(BlockingQueue<RawDrop> publishQueue) {
		this.publishQueue = publishQueue;
	}

	public SwiftRiverClient getApiClient() {
		return apiClient;
	}

	public void setApiClient(SwiftRiverClient apiClient) {
		this.apiClient = apiClient;
	}


	/**
	 * Publishes drops the the SwiftRiver REST API
	 * 
	 * Takes any drops that are in the publishQueue and posts them
	 * to the api in a single batch.
	 * 
	 * @throws IOException
	 */
	public void postDrops() throws IOException {
		if (publishQueue.isEmpty())
			return;

		List<RawDrop> rawDrops = new ArrayList<RawDrop>();
		publishQueue.drainTo(rawDrops);

		logger.debug(String.format("Posting %d drops to API", rawDrops.size()));

		// Convert instances of RawDrop in Drop for posting to API
		List<Drop> drops = new ArrayList<Drop>();
		for (RawDrop rawDrop : rawDrops) {
			Drop drop = new Drop();
			drop.setTitle(rawDrop.getTitle());
			drop.setContent(rawDrop.getContent());
			drop.setChannel(rawDrop.getChannel());
			drop.setDatePublished(rawDrop.getDatePublished());
			drop.setOriginalId(rawDrop.getDropOriginalId());
			drop.setRiverIds(rawDrop.getRiverIds());
			drop.setBucketIds(rawDrop.getBucketIds());
			drop.setMarkAsRead(rawDrop.getMarkAsRead());

			Drop.Identity identity = new Drop.Identity();
			identity.setAvatar(rawDrop.getIdentityAvatar());
			identity.setName(rawDrop.getIdentityName());
			identity.setOriginId(rawDrop.getIdentityOriginalId());
			identity.setUsername(rawDrop.getIdentityUsername());
			drop.setIdentity(identity);

			if (rawDrop.getLinks() != null) {
				List<Drop.Link> links = new ArrayList<Drop.Link>();
				for (RawDrop.Link l : rawDrop.getLinks()) {
					Drop.Link link = new Drop.Link();
					link.setUrl(l.getUrl());
					links.add(link);
				}
				drop.setLinks(links);
			}

			if (rawDrop.getTags() != null) {
				List<Drop.Tag> tags = new ArrayList<Drop.Tag>();
				for (RawDrop.Tag t : rawDrop.getTags()) {
					Drop.Tag tag = new Drop.Tag();
					tag.setTag(t.getName());
					tag.setType(t.getType());
					tags.add(tag);
				}
				drop.setTags(tags);
			}

			if (rawDrop.getMedia() != null) {
				List<Drop.Media> media = new ArrayList<Drop.Media>();
				for (RawDrop.Media rawMedia : rawDrop.getMedia()) {
					Drop.Media m = new Drop.Media();
					m.setUrl(rawMedia.getUrl());
					m.setType(rawMedia.getType());

					if (rawMedia.getThumbnails() != null) {
						List<Drop.Media.MediaThumbnail> thumbnails = new ArrayList<Drop.Media.MediaThumbnail>();
						for (RawDrop.Thumbnail t : rawMedia.getThumbnails()) {
							Drop.Media.MediaThumbnail thumbnail = new Drop.Media.MediaThumbnail();
							thumbnail.setUrl(t.getUrl());
							thumbnail.setSize(t.getSize());
							thumbnails.add(thumbnail);
						}
						m.setThumbnails(thumbnails);
					}					
					media.add(m);
				}
				drop.setMedia(media);
			}

			if (rawDrop.getPlaces() != null) {
				List<Drop.Place> places = new ArrayList<Drop.Place>();
				for(RawDrop.Place p : rawDrop.getPlaces()) {
					Drop.Place place = new Drop.Place();
					place.setName(p.getName());
					place.setLatitude(p.getLatitude());
					place.setLongitude(p.getLongitude());
					places.add(place);
				}
				drop.setPlaces(places);
			}
			drops.add(drop);
		}

		apiClient.drops().createDrops(drops);
	}
}
