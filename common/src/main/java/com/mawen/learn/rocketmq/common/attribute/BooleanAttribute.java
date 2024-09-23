package com.mawen.learn.rocketmq.common.attribute;

import static com.google.common.base.Preconditions.*;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/22
 */
public class BooleanAttribute extends Attribute{

	private final boolean defaultValue;

	public BooleanAttribute(String name, boolean changeable, boolean defaultValue) {
		super(name, changeable);
		this.defaultValue = defaultValue;
	}

	@Override
	public void verify(String value) {
		checkNotNull(value);

		if (!"false".equalsIgnoreCase(value) && !"true".equalsIgnoreCase(value)) {
			throw new RuntimeException("boolean attribute format is wrong.");
		}
	}

	public boolean isDefaultValue() {
		return defaultValue;
	}
}
