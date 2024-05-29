package com.mawen.learn.rocketmq4.spring.listener;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.mawen.learn.rocketmq4.spring.entity.AssetDocument;
import com.mawen.learn.rocketmq4.spring.entity.AssetHistoryDocument;
import com.mawen.learn.rocketmq4.spring.enums.PublishState;
import com.mawen.learn.rocketmq4.spring.repository.elasticsearch.AssetRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQTemplate;

import org.springframework.beans.BeanUtils;
import org.springframework.data.elasticsearch.NoSuchIndexException;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/27
 */
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(consumerGroup = "consumer-group-asset-b", topic = "asset-b")
public class BatchMessageListener implements RocketMQListener<List<String>> {

	public static final String REDIS_COUNT_KEY = "asset-b-count";

	private final AssetRepository assetRepository;
	private final RocketMQTemplate rocketMQTemplate;
	private final ElasticsearchTemplate elasticsearchTemplate;

	private final AtomicLong processCount = new AtomicLong(0);

	@Override
	public void onMessage(List<String> ids) {
		if (log.isTraceEnabled()) {
			log.trace("Consume message: {}", ids);
		}
		log.info("process {}", processCount.incrementAndGet());

		try {
			Iterable<AssetDocument> records = assetRepository.findAllById(ids);
			List<AssetDocument> docs = StreamSupport.stream(records.spliterator(), false)
					.peek(doc -> doc.setPublishState(PublishState.PUBLISHED))
					.collect(Collectors.toList());

			assetRepository.saveAll(docs);

			rocketMQTemplate.syncSend("asset-history-b", MessageBuilder.withPayload(ids).build());
		}
		catch (NoSuchIndexException e) {
			log.warn("Error occur {}", e.getMessage());
		}
	}
}
