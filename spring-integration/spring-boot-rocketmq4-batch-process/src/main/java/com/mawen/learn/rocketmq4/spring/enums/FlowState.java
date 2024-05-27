package com.mawen.learn.rocketmq4.spring.enums;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/27
 */
public enum FlowState {
	/**
	 * 处理中
	 */
	PROCESSING,

	/**
	 * 同意
	 */
	APPROVAL,

	/**
	 * 拒绝
	 */
	REJECTED;
}
