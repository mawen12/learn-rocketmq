package com.mawen.learn.rocketmq.client.consumer;

import java.util.List;

import com.mawen.learn.rocketmq.common.message.MessageExt;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/21
 */
@Getter
@Setter
@ToString
public class PopResult {
	private List<MessageExt> msgFoundList;
	private PopStatus popStatus;
	private long popTime;
	private long invisibleTime;
	private long restNum;

	public PopResult(PopStatus popStatus, List<MessageExt> msgFoundList) {
		this.popStatus = popStatus;
		this.msgFoundList = msgFoundList;
	}
}
