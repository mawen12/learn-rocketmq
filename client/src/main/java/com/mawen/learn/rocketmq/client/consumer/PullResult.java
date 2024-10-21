package com.mawen.learn.rocketmq.client.consumer;

import java.util.List;

import com.mawen.learn.rocketmq.common.message.MessageExt;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/21
 */
@AllArgsConstructor
@Getter
@Setter
@ToString
public class PullResult {
	private final PullStatus pullStatus;
	private final long nextBeginOffset;
	private final long minOffset;
	private final long maxOffset;
	private List<MessageExt> msgFoundList;
}
