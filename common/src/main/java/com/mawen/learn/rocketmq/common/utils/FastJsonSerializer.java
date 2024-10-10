package com.mawen.learn.rocketmq.common.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.support.config.FastJsonConfig;
import org.apache.commons.lang3.SerializationException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class FastJsonSerializer implements Serializer{

	private FastJsonConfig fastJsonConfig = new FastJsonConfig();

	public FastJsonConfig getFastJsonConfig() {
		return fastJsonConfig;
	}

	public void setFastJsonConfig(FastJsonConfig fastJsonConfig) {
		this.fastJsonConfig = fastJsonConfig;
	}

	@Override
	public <T> byte[] serialize(T t) throws SerializationException {
		if (t == null) {
			return new byte[0];
		}
		else {
			try {
				return JSON.toJSONBytes(this.fastJsonConfig.getCharset(), t, this.fastJsonConfig.getSerializeConfig(), this.fastJsonConfig.getSerializeFilters(), this.fastJsonConfig.getDateFormat(), JSON.DEFAULT_GENERATE_FEATURE, this.fastJsonConfig.getSerializerFeatures());
			}
			catch (Exception e) {
				throw new SerializationException("Could not serialize: " + e.getMessage(), e);
			}
		}
	}

	@Override
	public <T> T deserialize(byte[] bytes, Class<T> type) throws SerializationException {
		if (bytes != null && bytes.length != 0) {
			try {
				return JSON.parseObject(bytes, this.fastJsonConfig.getCharset(), type, this.fastJsonConfig.getParserConfig(), this.fastJsonConfig.getParseProcess(), JSON.DEFAULT_PARSER_FEATURE, this.fastJsonConfig.getFeatures());
			}
			catch (Exception e) {
				throw new SerializationException("Could not deserialize: " + e.getMessage(), e);
			}
		}
		return null;
	}
}
