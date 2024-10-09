package com.mawen.learn.rocketmq.common.metrics;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/9
 */
public enum MetricsExporterType {
	DISABLE(0),
	OLTP_GRPC(1),
	PROM(2),
	LOG(3);

	private final int value;

	MetricsExporterType(int value) {
		this.value = value;
	}

	public int getValue() {
		return value;
	}

	public boolean isEnable() {
		return this.value > 0;
	}

	public static MetricsExporterType valueOf(int value) {
		switch (value) {
			case 1:
				return OLTP_GRPC;
			case 2:
				return PROM;
			case 3:
				return LOG;
			default:
				return DISABLE;
		}
	}
}
