package com.mawen.learn.rocketmq.filter.expression;

import lombok.Getter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
@Getter
public class MQFilterException extends Exception{

	private static final long serialVersionID = 1L;

	private final int responseCode;
	private final String errorMessage;

	public MQFilterException(int responseCode, String errorMessage) {
		this.responseCode = responseCode;
		this.errorMessage = errorMessage;
	}

	public MQFilterException(String errorMessage, Throwable cause) {
		super(cause);
		this.responseCode = -1;
		this.errorMessage = errorMessage;
	}
}
