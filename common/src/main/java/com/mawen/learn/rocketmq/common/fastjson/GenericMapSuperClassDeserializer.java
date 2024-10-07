package com.mawen.learn.rocketmq.common.fastjson;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.parser.DefaultJSONParser;
import com.alibaba.fastjson.parser.JSONToken;
import com.alibaba.fastjson.parser.deserializer.MapDeserializer;
import com.alibaba.fastjson.parser.deserializer.ObjectDeserializer;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/7
 */
public class GenericMapSuperClassDeserializer implements ObjectDeserializer {

	private static final GenericMapSuperClassDeserializer INSTANCE = new GenericMapSuperClassDeserializer();

	@Override
	public <T> T deserialze(DefaultJSONParser parser, Type type, Object fieldName) {
		Class<?> clz = (Class<?>) type;
		Type genericSuperclass = clz.getGenericSuperclass();
		Map map;
		try {
			map = (Map) clz.newInstance();
		}
		catch (Exception e) {
			throw new JSONException("unsupported type " + type, e);
		}

		ParameterizedType parameterizedType = (ParameterizedType) genericSuperclass;
		Type keyType = parameterizedType.getActualTypeArguments()[0];
		Type valueType = parameterizedType.getActualTypeArguments()[1];

		if (String.class == keyType) {
			return (T) MapDeserializer.parseMap(parser, (Map<String, Object>)map, valueType, fieldName);
		}
		else {
			return (T) MapDeserializer.parseMap(parser, map, valueType, fieldName);
		}
	}

	@Override
	public int getFastMatchToken() {
		return JSONToken.LBRACE;
	}
}
