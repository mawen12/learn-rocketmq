package com.mawen.learn.rocketmq.common.consistenthash;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/7
 */
public interface HashFunction {

	long hash(String key);
}
