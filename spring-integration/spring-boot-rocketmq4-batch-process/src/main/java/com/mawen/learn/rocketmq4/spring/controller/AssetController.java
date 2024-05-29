package com.mawen.learn.rocketmq4.spring.controller;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Iterators;
import com.mawen.learn.rocketmq4.spring.entity.AssetDocument;
import com.mawen.learn.rocketmq4.spring.entity.AssetHistoryDocument;
import com.mawen.learn.rocketmq4.spring.enums.PublishState;
import com.mawen.learn.rocketmq4.spring.repository.elasticsearch.AssetHistoryRepository;
import com.mawen.learn.rocketmq4.spring.repository.elasticsearch.AssetRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.core.RocketMQTemplate;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static com.mawen.learn.rocketmq4.spring.listener.MessageListener.*;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/27
 */
@Slf4j
@RestController
@RequestMapping("/asset")
public class AssetController {

	@Autowired
	private AssetRepository assetRepository;

	@Autowired
	private RocketMQTemplate rocketMQTemplate;

	@Autowired
	private RedisTemplate<String, Long> redisTemplate;
	@Autowired
	private AssetHistoryRepository assetHistoryRepository;

	@PostMapping("/message")
	public String sendMessage() {
		log.info("Send message start....");

		redisTemplate.delete(REDIS_COUNT_KEY);

		Stream<AssetDocument> stream = assetRepository.streamByPublishState(PublishState.PROCESSING);
		Iterators.partition(stream.iterator(), 100).forEachRemaining(its -> {
			for (AssetDocument doc : its) {
				rocketMQTemplate.syncSend("asset-a", MessageBuilder.withPayload(doc.getId()).build());
			}
		});

		log.info("Send message end...");

		check();

		redisTemplate.delete(REDIS_COUNT_KEY);

		log.info("Check message finished...");
		return "Ok";
	}

	@PostMapping("/check")
	public String check() {

		int total = 0;
		long expectedCount = assetRepository.count();

		for (long i = getCountFromCache(); i < expectedCount; i = getCountFromCache()) {
			try {
				Thread.sleep(1000L);
			}
			catch (InterruptedException e) {
				throw new RuntimeException(e);
			}

			if (total != 0 && total % 10 == 0) {
				long esCount = assetHistoryRepository.count();
				if (esCount == expectedCount) {
					return "OK";
				}
			}

			total++;
		}

		return "OK";
	}

	private long getCountFromCache() {
		Long count = redisTemplate.opsForValue().get(REDIS_COUNT_KEY);
		return count == null ? 0L : count;
	}

	@PostMapping("/handle")
	public String handle() {

		Stream<AssetDocument> stream = assetRepository.streamByPublishState(PublishState.PROCESSING);
		Iterators.partition(stream.iterator(), 100).forEachRemaining(its -> {
			its.forEach(it -> it.setPublishState(PublishState.PUBLISHED));

			assetRepository.saveAll(its);

			List<AssetHistoryDocument> historyDocumentList = its.stream().map(doc -> {
						AssetHistoryDocument assetHistoryDocument = new AssetHistoryDocument();
						BeanUtils.copyProperties(doc, assetHistoryDocument);
						return assetHistoryDocument;
					})
					.collect(Collectors.toList());

			assetHistoryRepository.saveAll(historyDocumentList);
		});

		return "Ok";
	}

	@PostMapping("/message/batch/{count}")
	public String sendBatchMessage(@PathVariable("count") int count) {
		log.info("Send batch message start....");

		redisTemplate.delete(REDIS_COUNT_KEY);

		Stream<AssetDocument> stream = assetRepository.streamByPublishState(PublishState.PROCESSING);
		Iterators.partition(stream.iterator(), count).forEachRemaining(its -> {
			List<String> ids = its.stream().map(AssetDocument::getId).collect(Collectors.toList());
			rocketMQTemplate.syncSend("asset-b", MessageBuilder.withPayload(ids).build());
		});

		log.info("Send batch message end...");

		check();

		redisTemplate.delete(REDIS_COUNT_KEY);

		log.info("Check batch message finished...");
		return "Ok";
	}
}
