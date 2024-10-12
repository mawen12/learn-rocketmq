package com.mawen.learn.rocketmq.remoting.protocol.body;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public enum CMResult {

	CR_SUCCESS,

	CR_LATER,

	CR_ROLLBACK,

	CR_COMMIT,

	CR_THROW_EXCEPTION,

	CR_RETURN_NULL,
}
