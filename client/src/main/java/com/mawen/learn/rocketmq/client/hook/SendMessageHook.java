package com.mawen.learn.rocketmq.client.hook;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/18
 */
public interface SendMessageHook {

	String hookName();

	void sendMessageBefore(final SendMessageContext context);

	void sendMessageAfter(final SendMessageContext context);
}
