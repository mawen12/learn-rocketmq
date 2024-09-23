package com.mawen.learn.rocketmq.common.attribute;

import static java.lang.String.*;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/22
 */
public class LongRangeAttribute extends Attribute{

	private final long min;
	private final long max;
	private final long defaultValue;

	public LongRangeAttribute(String name, boolean changeable, long min, long max, long defaultValue) {
		super(name, changeable);
		this.min = min;
		this.max = max;
		this.defaultValue = defaultValue;
	}

	@Override
	public void verify(String value) {
		long l = Long.parseLong(value);
		if (l <= min || l >= max) {
			throw new RuntimeException(format("value is not in range(%d, %d)", min, max));
		}
	}

	public long getDefaultValue() {
		return defaultValue;
	}
}
