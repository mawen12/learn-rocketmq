package com.mawen.learn.rocketmq.filter.expression;

import java.util.Map;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
public interface EvaluationContext {

	Object get(String name);

	Map<String, Object> keyValues();
}
