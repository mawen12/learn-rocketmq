package com.mawen.learn.rocketmq.filter.expression;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
public interface BooleanExpression extends Expression{

	boolean matches(EvaluationContext context) throws Exception;
}
