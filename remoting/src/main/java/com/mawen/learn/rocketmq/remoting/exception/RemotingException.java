package com.mawen.learn.rocketmq.remoting.exception;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public class RemotingException extends Exception{

	private static final long serialVersionUID = 835200961127710180L;

	public RemotingException(String message) {
		super(message);
	}

	public RemotingException(String message, Throwable cause) {
		super(message, cause);
	}
}
