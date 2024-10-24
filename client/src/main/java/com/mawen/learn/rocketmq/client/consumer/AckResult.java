package com.mawen.learn.rocketmq.client.consumer;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
@Getter
@Setter
@ToString
public class AckResult {
	private AckStatus status;
	private String extraInfo;
	private long popTime;
}
