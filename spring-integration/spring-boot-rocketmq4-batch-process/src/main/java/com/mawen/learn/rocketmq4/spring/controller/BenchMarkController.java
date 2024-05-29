package com.mawen.learn.rocketmq4.spring.controller;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.Iterators;
import com.mawen.learn.rocketmq4.spring.entity.AssetDocument;
import com.mawen.learn.rocketmq4.spring.entity.AssetHistoryDocument;
import com.mawen.learn.rocketmq4.spring.entity.AssetIncDocument;
import com.mawen.learn.rocketmq4.spring.enums.PublishState;
import com.mawen.learn.rocketmq4.spring.repository.elasticsearch.AssetHistoryRepository;
import com.mawen.learn.rocketmq4.spring.repository.elasticsearch.AssetIncRepository;
import com.mawen.learn.rocketmq4.spring.repository.elasticsearch.AssetRepository;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.jeasy.random.EasyRandom;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/28
 */
@RestController
@RequestMapping("/benchmark")
@RequiredArgsConstructor
public class BenchMarkController {

	private final AssetRepository assetRepository;
	private final AssetIncRepository assetIncRepository;
	private final AssetHistoryRepository assetHistoryRepository;

	@PutMapping("/indexes/{count}")
	public String benchmarkMultiIndexes(@PathVariable("count") int count) throws InterruptedException {
		// clear
		assetRepository.deleteAll();
		assetIncRepository.deleteAll();
		assetHistoryRepository.deleteAll();

		// regenerate
		CountDownLatch latch = new CountDownLatch(2);
		ExecutorService executorService = Executors.newFixedThreadPool(3);

		executorService.submit(new AssetDocumentGenerate(latch, count, assetRepository::saveAll));
		executorService.submit(new AssetHistoryDocumentGenerate(latch, count, assetHistoryRepository::saveAll));
		executorService.submit(new AssetIncDocumentGenerate(latch, count, assetIncRepository::saveAll));

		latch.await();

		return "OK";
	}

	@PutMapping("/index/{count}")
	public String benchmarkIndex(@PathVariable("count") int count) throws InterruptedException {
		// clear
		assetRepository.deleteAll();

		// regenerate
		CountDownLatch latch = new CountDownLatch(2);
		ExecutorService executorService = Executors.newFixedThreadPool(3);

		executorService.submit(new AssetDocumentGenerate(latch, count, assetRepository::saveAll));
		executorService.submit(new AssetDocumentGenerate(latch, count, assetRepository::saveAll));
		executorService.submit(new AssetDocumentGenerate(latch, count, assetRepository::saveAll));

		latch.await();

		return "OK";
	}


	@AllArgsConstructor
	public static class AssetDocumentGenerate implements Runnable {

		private CountDownLatch latch;
		private int count;
		private Consumer<List<AssetDocument>> consumer;


		@Override
		public void run() {
			EasyRandom random = new EasyRandom();

			Stream<AssetDocument> stream = IntStream.range(0, count).mapToObj(i -> {
				AssetDocument doc = random.nextObject(AssetDocument.class);
				doc.setId(null);
				doc.setPublishState(PublishState.PROCESSING);
				return doc;
			});

			Iterators.partition(stream.iterator(), 10000).forEachRemaining(consumer);

			latch.countDown();
		}
	}

	@AllArgsConstructor
	public static class AssetHistoryDocumentGenerate implements Runnable {

		private CountDownLatch latch;
		private int count;
		private Consumer<List<AssetHistoryDocument>> consumer;


		@Override
		public void run() {
			EasyRandom random = new EasyRandom();

			Stream<AssetHistoryDocument> stream = IntStream.range(0, count).mapToObj(i -> {
				AssetHistoryDocument doc = random.nextObject(AssetHistoryDocument.class);
				doc.setId(null);
				doc.setPublishState(PublishState.PROCESSING);
				return doc;
			});

			Iterators.partition(stream.iterator(), 10000).forEachRemaining(consumer);

			latch.countDown();
		}
	}

	@AllArgsConstructor
	public static class AssetIncDocumentGenerate implements Runnable {

		private CountDownLatch latch;
		private int count;
		private Consumer<List<AssetIncDocument>> consumer;


		@Override
		public void run() {
			EasyRandom random = new EasyRandom();

			Stream<AssetIncDocument> stream = IntStream.range(0, count).mapToObj(i -> {
				AssetIncDocument doc = random.nextObject(AssetIncDocument.class);
				doc.setId(null);
				doc.setPublishState(PublishState.PROCESSING);
				return doc;
			});

			Iterators.partition(stream.iterator(), 10000).forEachRemaining(consumer);

			latch.countDown();
		}
	}


}
