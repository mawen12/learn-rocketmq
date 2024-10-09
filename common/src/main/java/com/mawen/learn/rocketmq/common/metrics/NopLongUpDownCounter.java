package com.mawen.learn.rocketmq.common.metrics;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.context.Context;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/9
 */
public class NopLongUpDownCounter implements LongUpDownCounter {

	@Override
	public void add(long l) {

	}

	@Override
	public void add(long l, Attributes attributes) {

	}

	@Override
	public void add(long l, Attributes attributes, Context context) {

	}
}
