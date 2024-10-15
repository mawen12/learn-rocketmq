package com.mawen.learn.rocketmq.remoting.metrics;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import com.google.common.collect.Lists;
import com.mawen.learn.rocketmq.common.Pair;
import com.mawen.learn.rocketmq.common.attribute.Attribute;
import com.mawen.learn.rocketmq.common.metrics.NopLongHistogram;
import io.netty.util.concurrent.Future;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.View;
import io.opentelemetry.sdk.metrics.ViewBuilder;

import static com.mawen.learn.rocketmq.remoting.metrics.RemotingMetricsConstant.*;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/15
 */
public class RemotingMetricsManager {

	public static LongHistogram rpcLatency = new NopLongHistogram();

	public static Supplier<AttributesBuilder> attributesBuilderSupplier;

	public static AttributesBuilder newAttributesBuilder() {
		if (attributesBuilderSupplier == null) {
			return Attributes.builder();
		}
		return attributesBuilderSupplier.get().put(LABEL_PROTOCOL_TYPE, PROTOCOL_TYPE_REMOTING);
	}

	public static void initMetrics(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier) {
		RemotingMetricsManager.attributesBuilderSupplier = attributesBuilderSupplier;
		rpcLatency = meter.histogramBuilder(HISTOGRAM_RPC_LATENCY)
				.setDescription("Rpc latency")
				.setUnit("milliseconds")
				.ofLongs()
				.build();
	}

	public static List<Pair<InstrumentSelector, ViewBuilder>> getMetricsView() {
		List<Double> rpcCostTimeBuckets = Arrays.asList((double) Duration.ofMillis(1).toMillis(),
				(double) Duration.ofMillis(3).toMillis(),
				(double) Duration.ofMillis(5).toMillis(),
				(double) Duration.ofMillis(7).toMillis(),
				(double) Duration.ofMillis(10).toMillis(),
				(double) Duration.ofMillis(100).toMillis(),
				(double) Duration.ofSeconds(1).toMillis(),
				(double) Duration.ofSeconds(2).toMillis(),
				(double) Duration.ofSeconds(3).toMillis());

		InstrumentSelector selector = InstrumentSelector.builder()
				.setType(InstrumentType.HISTOGRAM)
				.setName(HISTOGRAM_RPC_LATENCY)
				.build();

		ViewBuilder viewBuilder = View.builder().setAggregation(Aggregation.explicitBucketHistogram(rpcCostTimeBuckets));

		return Lists.newArrayList(new Pair<>(selector, viewBuilder));
	}

	public static String getWriteAndFlushResult(Future<?> future) {
		String result = RESULT_SUCCESS;
		if (future.isCancelled()) {
			result = RESULT_CANCELED;
		}
		else if (!future.isSuccess()) {
			result = RESULT_WRITE_CHANNEL_FAILED;
		}
		return result;
	}
}
