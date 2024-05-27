package com.mawen.learn.rocketmq4.spring.listener;

import java.util.concurrent.atomic.AtomicLong;

import com.mawen.learn.rocketmq4.spring.entity.AssetHistoryDocument;
import com.mawen.learn.rocketmq4.spring.event.GenerateEvent;
import com.mawen.learn.rocketmq4.spring.repository.elasticsearch.AssetHistoryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;

import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import static com.mawen.learn.rocketmq4.spring.listener.MessageListener.*;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/27
 */
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(consumerGroup = "consumer-group-asset-history-a", topic = "asset-history-a")
public class HistoryMessageListener implements RocketMQListener<AssetHistoryDocument> {

	private final RedisTemplate<String, Long> redisTemplate;
	private final AssetHistoryRepository assetHistoryRepository;

	private final AtomicLong processCount = new AtomicLong(0);


	@Override
	public void onMessage(AssetHistoryDocument message) {
		log.info("process {}", processCount.incrementAndGet());

		assetHistoryRepository.save(message);

		randomFailed(50);

		redisTemplate.opsForValue().increment(REDIS_COUNT_KEY);

		randomFailed(30);
	}

	@EventListener(GenerateEvent.class)
	public void onGenerateEvent(GenerateEvent event) {
		processCount.set(0L);
	}

	public void randomFailed(int randomPoint) {
		if (processCount.get() % randomPoint == 0) {
			throw new RuntimeException("Random Failed");
		}
	}
}
