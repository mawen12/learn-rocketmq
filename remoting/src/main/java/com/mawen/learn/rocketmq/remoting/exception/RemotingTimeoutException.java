package com.mawen.learn.rocketmq.remoting.exception;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public class RemotingTimeoutException extends RemotingException{

	private static final long serialVersionUID = 607858412446259744L;

	public RemotingTimeoutException(String message) {
		super(message);
	}

	public RemotingTimeoutException(String addr, long timeoutMillis) {
		this(addr, timeoutMillis, null);
	}

	public RemotingTimeoutException(String addr, long timeoutMillis, Throwable cause) {
		super("wait response on the channel <" + addr + "> timeout, " + timeoutMillis + "(ms)", cause);
	}
}
