package com.mawen.learn.rocketmq.remoting.protocol;

import java.util.Map;

import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import io.netty.buffer.ByteBuf;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public interface FastCodesHeader {

	default String getAndCheckNotNull(Map<String, String> fields, String field) {
		String value = fields.get(field);
		if (value == null) {
			String headerClass = this.getClass().getSimpleName();
			RemotingCommand.log.error("the custom field {}.{} is null", headerClass, field);
		}
		return value;
	}

	default void writeIfNotNull(ByteBuf out, String key, Object value) {
		if (value != null) {
			RocketMQSerializable.writeStr(out, true, key);
			RocketMQSerializable.writeStr(out, false, value.toString());
		}
	}

	void encode(ByteBuf out);

	void decode(Map<String, String> fields) throws RemotingCommandException;

}
