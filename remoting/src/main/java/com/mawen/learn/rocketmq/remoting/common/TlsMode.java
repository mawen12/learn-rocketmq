package com.mawen.learn.rocketmq.remoting.common;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/15
 */
public enum TlsMode {
	DISABLED("disabled"),

	PERMISSIVE("permissive"),

	ENFORCING("enforcing");

	private final String name;

	TlsMode(String name) {
		this.name = name;
	}

	public static TlsMode parse(String mode) {
		for (TlsMode value : TlsMode.values()) {
			if (value.getName().equals(mode)) {
				return value;
			}
		}
		return PERMISSIVE;
	}

	public String getName() {
		return name;
	}
}
