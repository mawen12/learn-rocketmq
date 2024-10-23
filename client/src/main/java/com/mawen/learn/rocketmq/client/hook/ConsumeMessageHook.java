package com.mawen.learn.rocketmq.client.hook;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/23
 */
public interface ConsumeMessageHook {

	String hookName();

	void consumeMessageBefore(final ConsumeMessageContext context);

	void consumeMessageAfter(final ConsumeMessageContext context);
}
