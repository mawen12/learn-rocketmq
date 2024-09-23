package com.mawen.learn.rocketmq.common.attribute;

import java.util.Set;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/22
 */
public class EnumAttribute extends Attribute{

	private final Set<String> universe;
	private final String defaultValue;

	public EnumAttribute(String name, boolean changeable, Set<String> universe, String defaultValue) {
		super(name, changeable);
		this.universe = universe;
		this.defaultValue = defaultValue;
	}

	@Override
	public void verify(String value) {
		if (!this.universe.contains(value)) {
			throw new RuntimeException("value is not in set: " + value);
		}
	}

	public String getDefaultValue() {
		return defaultValue;
	}
}
