package com.mawen.learn.rocketmq.store.metrics;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import com.google.common.collect.Lists;
import com.mawen.learn.rocketmq.common.Pair;
import com.mawen.learn.rocketmq.common.metrics.NopLongCounter;
import com.mawen.learn.rocketmq.common.metrics.NopLongHistogram;
import com.mawen.learn.rocketmq.common.metrics.NopObservableLongGauge;
import com.mawen.learn.rocketmq.store.DefaultMessageStore;
import com.mawen.learn.rocketmq.store.config.MessageStoreConfig;
import com.mawen.learn.rocketmq.store.timer.Slot;
import com.mawen.learn.rocketmq.store.timer.TimerMessageStore;
import com.mawen.learn.rocketmq.store.timer.TimerMetrics;
import com.mawen.learn.rocketmq.store.timer.TimerWheel;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.View;
import io.opentelemetry.sdk.metrics.ViewBuilder;

import static com.mawen.learn.rocketmq.store.metrics.DefaultStoreMetricsConstant.*;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/17
 */
public class DefaultStoreMetricsManager {
	public static Supplier<AttributesBuilder> attributesBuilderSupplier;
	public static MessageStoreConfig messageStoreConfig;

	public static ObservableLongGauge storageSize = new NopObservableLongGauge();
	public static ObservableLongGauge flushBehind = new NopObservableLongGauge();
	public static ObservableLongGauge dispatchBehind = new NopObservableLongGauge();
	public static ObservableLongGauge messageReserveTime = new NopObservableLongGauge();

	public static ObservableLongGauge timerEnqueueLag = new NopObservableLongGauge();
	public static ObservableLongGauge timerEnqueueLatency = new NopObservableLongGauge();
	public static ObservableLongGauge timerDequeueLag = new NopObservableLongGauge();
	public static ObservableLongGauge timerDequeueLatency = new NopObservableLongGauge();
	public static ObservableLongGauge timingMessages = new NopObservableLongGauge();

	public static LongCounter timerDequeueTotal = new NopLongCounter();
	public static LongCounter timerEnqueueTotal = new NopLongCounter();
	public static ObservableLongGauge timerMessageSnapshot = new NopObservableLongGauge();
	public static LongHistogram timerMessageSetLatency = new NopLongHistogram();

	public static List<Pair<InstrumentSelector, ViewBuilder>> getMetricsView() {
		List<Double> rpcCostTimeBuckets = Arrays.asList(
				1d * 1 * 1 * 60,
				1d * 1 * 10 * 60,
				1d * 1 * 60 * 60,
				1d * 12 * 60 * 60,
				1d * 24 * 60 * 60,
				3d * 24 * 60 * 60
		);

		InstrumentSelector selector = InstrumentSelector.builder()
				.setType(InstrumentType.HISTOGRAM)
				.setName(HISTOGRAM_DELAY_MSG_LATENCY)
				.build();

		ViewBuilder viewBuilder = View.builder()
				.setAggregation(Aggregation.explicitBucketHistogram(rpcCostTimeBuckets));
		return Lists.newArrayList(new Pair<>(selector, viewBuilder));
	}

	public static void init(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier, DefaultMessageStore messageStore) {
		DefaultStoreMetricsManager.attributesBuilderSupplier = attributesBuilderSupplier;
		DefaultStoreMetricsManager.messageStoreConfig = messageStore.getMessageStoreConfig();

		storageSize = meter.gaugeBuilder(GAUGE_STORAGE_SIZE)
				.setDescription("Broker storage size")
				.setUnit("bytes")
				.ofLongs()
				.buildWithCallback(measurement -> {
					File storeDir = new File(messageStoreConfig.getStorePathRootDir());
					if (storeDir.exists() && storeDir.isDirectory()) {
						long totalSpace = storeDir.getTotalSpace();
						if (totalSpace > 0) {
							measurement.record(totalSpace - storeDir.getFreeSpace(), newAttributesBuilder().build());
						}
					}
				});

		flushBehind = meter.gaugeBuilder(GAUGE_STORAGE_FLUSH_BEHIND)
				.setDescription("Broker flush behind bytes")
				.setUnit("bytes")
				.ofLongs()
				.buildWithCallback(measurement -> measurement.record(messageStore.flushBehindBytes(), newAttributesBuilder().build()));

		dispatchBehind = meter.gaugeBuilder(GAUGE_STORAGE_DISPATCH_BEHIND)
				.setDescription("Broker dispatch behind bytes")
				.setUnit("bytes")
				.ofLongs()
				.buildWithCallback(measurement -> measurement.record(messageStore.dispatchBehindBytes(), newAttributesBuilder().build()));

		messageReserveTime = meter.gaugeBuilder(GAUGE_STORAGE_MESSAGE_RESERVE_TIME)
				.setDescription("Broker message reserve time")
				.setUnit("milliseconds")
				.ofLongs()
				.buildWithCallback(measurement -> {
					long earliestMessageTime = messageStore.getEarliestMessageTime();
					if (earliestMessageTime <= 0) {
						return;
					}
					measurement.record(System.currentTimeMillis() - earliestMessageTime, newAttributesBuilder().build());
				});

		if (messageStore.getMessageStoreConfig().isTimerWheelEnable()) {
			timerEnqueueLag = meter.gaugeBuilder(GAUGE_TIMER_ENQUEUE_LAG)
					.setDescription("Timer enqueue messages lag")
					.ofLongs()
					.buildWithCallback(measurement -> {
						TimerMessageStore timerMessageStore = messageStore.getTimerMessageStore();
						measurement.record(timerMessageStore.getEnqueueBehindMessages(), newAttributesBuilder().build());
					});

			timerEnqueueLatency = meter.gaugeBuilder(GAUGE_TIMER_ENQUEUE_LATENCY)
					.setDescription("Timer enqueue latency")
					.setUnit("milliseconds")
					.ofLongs()
					.buildWithCallback(measurement -> {
						TimerMessageStore timerMessageStore = messageStore.getTimerMessageStore();
						measurement.record(timerMessageStore.getEnqueueBehindMillis(), newAttributesBuilder().build());
					});

			timerDequeueLag = meter.gaugeBuilder(GAUGE_TIMER_DEQUEUE_LAG)
					.setDescription("Timer dequeue messages lag")
					.ofLongs()
					.buildWithCallback(measurement -> {
						TimerMessageStore timerMessageStore = messageStore.getTimerMessageStore();
						measurement.record(timerMessageStore.getDequeueBehindMessages(), newAttributesBuilder().build());
					});

			timerDequeueLatency = meter.gaugeBuilder(GAUGE_TIMER_DEQUEUE_LATENCY)
					.setDescription("Timer dequeue latency")
					.setUnit("milliseconds")
					.ofLongs()
					.buildWithCallback(measurement -> {
						TimerMessageStore timerMessageStore = messageStore.getTimerMessageStore();
						measurement.record(timerMessageStore.getDequeueBehind(), newAttributesBuilder().build());
					});

			timingMessages = meter.gaugeBuilder(GAUGE_TIMING_MESSAGES)
					.setDescription("Current message number is timing")
					.ofLongs()
					.buildWithCallback(measurement -> {
						TimerMessageStore timerMessageStore = messageStore.getTimerMessageStore();
						timerMessageStore.getTimerMetrics()
								.getTimingCount()
								.forEach((topic, metric) -> {
									measurement.record(metric.getCount().get(), newAttributesBuilder().put(LABEL_TOPIC, topic).build());
								});
					});

			timerDequeueTotal = meter.counterBuilder(COUNTER_TIMER_DEQUEUE_TOTAL)
					.setDescription("Total number of timer dequeue")
					.build();
			timerEnqueueTotal = meter.counterBuilder(COUNTER_TIMER_ENQUEUE_TOTAL)
					.setDescription("Total number of timer enqueue")
					.build();

			timerMessageSnapshot = meter.gaugeBuilder(GAUGE_TIMER_MESSAGE_SNAPSHOT)
					.setDescription("Timer message distribution snapshot, only count timing messages in 24h.")
					.ofLongs()
					.buildWithCallback(measurement -> {
						TimerMetrics timerMetrics = messageStore.getTimerMessageStore().getTimerMetrics();
						TimerWheel timerWheel = messageStore.getTimerMessageStore().getTimerWheel();
						int precisionMs = messageStoreConfig.getTimerPrecisionMs();
						List<Integer> timerDist = timerMetrics.getTimerDist();
						long curTime = System.currentTimeMillis() / precisionMs * precisionMs;

						for (int i = 0; i < timerDist.size(); i++) {
							int slotBeforeNum = i == 0 ? 9 : timerDist.get(i - 1) * 1000 / precisionMs;
							int slotTotalNum = timerDist.get(i) * 1000 / precisionMs;
							int periodTotal  = 0;
							for (int j = slotBeforeNum; j < slotTotalNum; j++) {
								Slot slotEach = timerWheel.getSlot(curTime + (long) j * precisionMs);
								periodTotal += slotEach.num;
							}
							measurement.record(periodTotal, newAttributesBuilder().put(LABEL_TIMING_BOUND, timerDist.get(i).toString()).build());
						}
					});

			timerMessageSetLatency = meter.histogramBuilder(HISTOGRAM_DELAY_MSG_LATENCY)
					.setDescription("Timer message set latency distribution")
					.setUnit("seconds")
					.ofLongs()
					.build();
		}
	}

	public static void incTimerDequeueCount(String topic) {
		timerDequeueTotal.add(1, newAttributesBuilder().put(LABEL_TOPIC,topic).build());
	}

	public static void incTimerEnqueueCount(String topic) {
		AttributesBuilder attributesBuilder = newAttributesBuilder();
		if (topic != null) {
			attributesBuilder.put(LABEL_TOPIC, topic);
		}
		timerEnqueueTotal.add(1, attributesBuilder.build());
	}

	public static AttributesBuilder newAttributesBuilder() {
		if (attributesBuilderSupplier == null) {
			return Attributes.builder();
		}

		return attributesBuilderSupplier.get()
				.put(LABEL_STORAGE_TYPE, DEFAULT_STORAGE_TYPE)
				.put(LABEL_STORAGE_MEDIUM, DEFAULT_STORAGE_MEDIUM);
	}
}
