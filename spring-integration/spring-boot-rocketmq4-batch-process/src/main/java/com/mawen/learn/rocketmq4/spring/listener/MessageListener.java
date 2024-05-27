package com.mawen.learn.rocketmq4.spring.listener;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.databind.util.BeanUtil;
import com.mawen.learn.rocketmq4.spring.entity.AssetDocument;
import com.mawen.learn.rocketmq4.spring.entity.AssetHistoryDocument;
import com.mawen.learn.rocketmq4.spring.enums.PublishState;
import com.mawen.learn.rocketmq4.spring.event.GenerateEvent;
import com.mawen.learn.rocketmq4.spring.repository.elasticsearch.AssetRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQTemplate;

import org.springframework.beans.BeanUtils;
import org.springframework.context.event.EventListener;
import org.springframework.data.elasticsearch.NoSuchIndexException;
import org.springframework.data.elasticsearch.client.elc.ChildTemplate;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.client.elc.IndicesTemplate;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/27
 */
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(consumerGroup = "consumer-group-asset-a", topic = "asset-a")
public class MessageListener implements RocketMQListener<String> {

	public static final String REDIS_COUNT_KEY = "asset-count";

	private final AssetRepository assetRepository;
	private final RocketMQTemplate rocketMQTemplate;
	private final ElasticsearchTemplate elasticsearchTemplate;

	private final AtomicLong processCount = new AtomicLong(0);

	@Override
	public void onMessage(String id) {
		if (log.isTraceEnabled()) {
			log.trace("Consume message: {}", id);
		}
		log.info("process {}", processCount.incrementAndGet());

		randomFailed(1, false, id, true);

		try {
			Optional<AssetDocument> optional = assetRepository.findById(id);
			if (optional.isPresent()) {
				optional.ifPresent(doc -> {
					doc.setPublishState(PublishState.PUBLISHED);
					assetRepository.save(doc);

					AssetHistoryDocument assetHistoryDocument = new AssetHistoryDocument();
					BeanUtils.copyProperties(doc, assetHistoryDocument);
					rocketMQTemplate.syncSend("asset-history-a", MessageBuilder.withPayload(assetHistoryDocument).build());
				});
			}

		}
		catch (NoSuchIndexException e) {
			log.warn("Error occur {}, event content: {}", e.getMessage(), id);
		}
	}

	@EventListener(GenerateEvent.class)
	public void onGenerateEvent(GenerateEvent event) {
		processCount.set(0L);
	}

	public void randomFailed(int randomPoint, boolean deleteIndices, String id, boolean deleteRecord) {
		if (processCount.get() % randomPoint == 0) {
			if (deleteIndices) {
				elasticsearchTemplate.indexOps(AssetDocument.class).delete();
			}
			else if (deleteRecord) {
				assetRepository.deleteById(id);
			}
			else {
				throw new RuntimeException("Random Failed");
			}
		}
	}
}
