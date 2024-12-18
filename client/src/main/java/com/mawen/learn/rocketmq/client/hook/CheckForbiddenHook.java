package com.mawen.learn.rocketmq.client.hook;

import com.mawen.learn.rocketmq.client.exception.MQClientException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
public interface CheckForbiddenHook {

	String hookName();

	void checkForbidden(final CheckForbiddenContext context) throws MQClientException;
}
