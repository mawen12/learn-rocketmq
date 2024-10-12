package com.mawen.learn.rocketmq.remoting.protocol;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.BitSet;

import com.alibaba.fastjson.parser.DefaultJSONParser;
import com.alibaba.fastjson.parser.JSONToken;
import com.alibaba.fastjson.parser.deserializer.ObjectDeserializer;
import com.alibaba.fastjson.serializer.JSONSerializer;
import com.alibaba.fastjson.serializer.ObjectSerializer;
import com.alibaba.fastjson.serializer.SerializeWriter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class BitSetSerializerDeserializer implements ObjectSerializer, ObjectDeserializer {

	@Override
	public int getFastMatchToken() {
		return JSONToken.LITERAL_STRING;
	}

	@Override
	public void write(JSONSerializer serializer, Object object, Object fieldName, Type fieldType, int features) throws IOException {
		SerializeWriter out = serializer.out;
		out.writeByteArray(((BitSet)object).toByteArray());
	}

	@Override
	public <T> T deserialze(DefaultJSONParser parser, Type type, Object fieldName) {
		byte[] bytes = parser.parseObject(byte[].class);
		if (bytes != null) {
			return (T) BitSet.valueOf(bytes);
		}
		return null;
	}
}
