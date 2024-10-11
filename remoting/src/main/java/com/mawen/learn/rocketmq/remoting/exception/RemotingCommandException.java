package com.mawen.learn.rocketmq.remoting.exception;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public class RemotingCommandException extends RemotingException{

	private static final long serialVersionUID = -7471147515676429891L;

	public RemotingCommandException(String message) {
		super(message);
	}

	public RemotingCommandException(String message, Throwable cause) {
		super(message, cause);
	}
}
