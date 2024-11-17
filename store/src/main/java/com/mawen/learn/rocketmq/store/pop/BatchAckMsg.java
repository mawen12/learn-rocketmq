package com.mawen.learn.rocketmq.store.pop;

import java.util.ArrayList;
import java.util.List;

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
public class BatchAckMsg extends AckMsg {

	@JSONField(name = "aol", alternateNames = {"ackOffsetList"})
	private List<Long> ackOffsetList = new ArrayList<>(32);
}
