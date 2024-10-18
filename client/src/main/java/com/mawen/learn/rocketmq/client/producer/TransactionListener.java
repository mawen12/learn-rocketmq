package com.mawen.learn.rocketmq.client.producer;

import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.common.message.MessageExt;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/18
 */
public interface TransactionListener {

	LocalTransactionState executeLocalTransaction(final Message msg, final Object arg);

	LocalTransactionState checkLockTransaction(final MessageExt msg);
}
