package com.mawen.learn.rocketmq.common.metrics;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.context.Context;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/9
 */
public class NopLongHistogram implements LongHistogram {

	@Override
	public void record(long l) {

	}

	@Override
	public void record(long l, Attributes attributes) {

	}

	@Override
	public void record(long l, Attributes attributes, Context context) {

	}
}
