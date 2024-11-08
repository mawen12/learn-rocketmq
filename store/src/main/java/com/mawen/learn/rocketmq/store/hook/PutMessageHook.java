package com.mawen.learn.rocketmq.store.hook;

import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.store.PutMessageResult;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/8
 */
public interface PutMessageHook {

	String hookName();

	PutMessageResult executeBeforePutMessage(MessageExt msg);
}
