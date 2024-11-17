package com.mawen.learn.rocketmq.store.pop;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/17
 */
@Getter
@Setter
@ToString
public class AckMsg {

	@JSONField(name = "ao", alternateNames = {"ackOffset"})
	private long ackOffset;

	@JSONField(name = "so", alternateNames = {"startOffset"})
	private long startOffset;

	@JSONField(name = "c", alternateNames = {"consumerGroup"})
	private String consumerGroup;

	@JSONField(name = "t", alternateNames = {"topic"})
	private String topic;

	@JSONField(name = "q", alternateNames = {"queueId"})
	private int queueId;

	@JSONField(name = "pt", alternateNames = {"popTime"})
	private long popTime;

	@JSONField(name = "bn", alternateNames = {"brokerName"})
	private String brokerName;
}
