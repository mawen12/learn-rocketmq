package com.mawen.learn.rocketmq.store.metrics;

import java.util.function.Supplier;

import com.mawen.learn.rocketmq.common.metrics.NopObservableDoubleGauge;
import com.mawen.learn.rocketmq.common.metrics.NopObservableLongGauge;
import com.mawen.learn.rocketmq.store.config.MessageStoreConfig;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.api.metrics.ObservableLongGauge;

import static com.mawen.learn.rocketmq.store.metrics.DefaultStoreMetricsConstant.*;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/17
 */
public class RocksDBStoreMetricsManager {

	public static Supplier<AttributesBuilder> attributesBuilderSupplier;
	public static MessageStoreConfig messageStoreConfig;

	public static ObservableLongGauge bytesRocksdbRead = new NopObservableLongGauge();

	public static ObservableLongGauge bytesRocksdbWritten = new NopObservableLongGauge();

	public static ObservableLongGauge timesRocksdbRead = new NopObservableLongGauge();

	public static ObservableLongGauge timesRocksdbWrittenSelf = new NopObservableLongGauge();

	public static ObservableLongGauge timesRocksdbWrittenOther = new NopObservableLongGauge();

	public static ObservableLongGauge timesRocksdbWrittenCompressed = new NopObservableLongGauge();

	public static ObservableDoubleGauge bytesRocksdbAmplificationRead = new NopObservableDoubleGauge();

	public static ObservableDoubleGauge rocksdbCacheHitRate = new NopObservableDoubleGauge();

	public static volatile long blockCacheHitTimes = 0;
	public static volatile long blockCacheMissTimes = 0;

	public static void init(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier, RocksDBMessageStore messageStore) {

	}

	public static AttributesBuilder newAttributesBuilder () {
		if (attributesBuilderSupplier == null) {
			return Attributes.builder();
		}
		return attributesBuilderSupplier.get()
				.put(LABEL_STORAGE_TYPE, DEFAULT_STORAGE_TYPE)
				.put(LABEL_STORAGE_MEDIUM, DEFAULT_STORAGE_MEDIUM);
	}
}
