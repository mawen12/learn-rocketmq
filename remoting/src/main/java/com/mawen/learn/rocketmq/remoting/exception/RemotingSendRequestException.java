package com.mawen.learn.rocketmq.remoting.exception;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public class RemotingSendRequestException extends RemotingException{

	private static final long serialVersionUID = -5717145716278094756L;

	public RemotingSendRequestException(String addr) {
		this(addr, null);
	}

	public RemotingSendRequestException(String addr, Throwable cause) {
		super("send request to <" + addr + "> failed", cause);
	}
}
