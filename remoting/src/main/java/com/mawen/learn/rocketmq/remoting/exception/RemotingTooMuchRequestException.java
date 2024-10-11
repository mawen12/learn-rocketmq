package com.mawen.learn.rocketmq.remoting.exception;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public class RemotingTooMuchRequestException extends RemotingException{

	private static final long serialVersionUID = -3634935396876349949L;

	public RemotingTooMuchRequestException(String message) {
		super(message);
	}
}
