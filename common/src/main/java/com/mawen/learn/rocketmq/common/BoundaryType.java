package com.mawen.learn.rocketmq.common;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/1
 */
public enum BoundaryType {
	LOWER("lower"),

	UPPER("upper");

	private String name;

	BoundaryType(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public static BoundaryType getType(String name) {
		if (UPPER.name.equals(name)) {
			return UPPER;
		}
		return LOWER;
	}
}
