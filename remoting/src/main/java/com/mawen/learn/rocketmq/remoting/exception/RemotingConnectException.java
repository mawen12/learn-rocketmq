package com.mawen.learn.rocketmq.remoting.exception;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public class RemotingConnectException extends RemotingException{

	private static final long serialVersionUID = -6305056221342190943L;

	public RemotingConnectException(String addr) {
		this(addr, null);
	}

	public RemotingConnectException(String addr, Throwable cause) {
		super("connect to " + addr + " failed", cause);
	}
}
