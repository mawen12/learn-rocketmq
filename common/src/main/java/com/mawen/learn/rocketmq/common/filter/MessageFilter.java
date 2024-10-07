package com.mawen.learn.rocketmq.common.filter;

import com.mawen.learn.rocketmq.common.message.MessageExt;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/7
 */
public interface MessageFilter {

	boolean match(final MessageExt msg, final FilterContext context);
}
