package com.mawen.learn.rocketmq.filter.expression;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
public interface Expression {

	Object evaluate(EvaluationContext context) throws Exception;
}
