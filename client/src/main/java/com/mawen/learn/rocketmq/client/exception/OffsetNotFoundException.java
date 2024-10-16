package com.mawen.learn.rocketmq.client.exception;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
public class OffsetNotFoundException extends MQBrokerException{

	public OffsetNotFoundException() {
		super();
	}

	public OffsetNotFoundException(int responseCode, String errorMessage) {
		super(responseCode, errorMessage);
	}

	public OffsetNotFoundException(int responseCode, String errorMessage, String brokerAddr) {
		super(responseCode, errorMessage, brokerAddr);
	}
}
