package com.mawen.learn.rocketmq4.spring.controller;

import java.util.ArrayList;
import java.util.List;

import com.mawen.learn.rocketmq4.spring.entity.AssetDocument;
import com.mawen.learn.rocketmq4.spring.enums.PublishState;
import com.mawen.learn.rocketmq4.spring.event.GenerateEvent;
import com.mawen.learn.rocketmq4.spring.repository.elasticsearch.AssetHistoryRepository;
import com.mawen.learn.rocketmq4.spring.repository.elasticsearch.AssetRepository;
import lombok.RequiredArgsConstructor;
import org.jeasy.random.EasyRandom;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/27
 */
@RestController
@RequestMapping("/generate")
@RequiredArgsConstructor
public class GenerateController {

	private static final String TOPIC = "demo";

	private final AssetRepository assetRepository;
	private final AssetHistoryRepository assetHistoryRepository;
	private final ApplicationContext applicationContext;

	@PutMapping("/{count}")
	public String generateRecords(@PathVariable("count") int count) {
		// clear
		assetRepository.deleteAll();
		assetHistoryRepository.deleteAll();

		// regenerate
		EasyRandom random = new EasyRandom();

		List<AssetDocument> list = new ArrayList<>(count);
		for (int i = 0; i < count; i++) {
			AssetDocument doc = random.nextObject(AssetDocument.class);
			doc.setId(null);
			doc.setTopicCode(TOPIC);
			doc.setPublishState(PublishState.PROCESSING);
			list.add(doc);
		}

		assetRepository.saveAll(list);

		applicationContext.publishEvent(new GenerateEvent());

		return "OK";
	}
}
