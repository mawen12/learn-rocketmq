package com.mawen.learn.rocketmq.common.utils;

import org.apache.commons.lang3.SerializationException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public interface Serializer {

	<T> byte[] serialize(T t) throws SerializationException;

	<T> T deserialize(byte[] bytes, Class<T> type) throws SerializationException;
}
