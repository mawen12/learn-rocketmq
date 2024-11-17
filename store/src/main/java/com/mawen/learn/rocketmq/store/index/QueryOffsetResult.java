package com.mawen.learn.rocketmq.store.index;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/17
 */
@Getter
@AllArgsConstructor
public class QueryOffsetResult {
	private final List<Long> phyOffsets;
	private final long indexLastUpdateTimestamp;
	private final long indexLastUpdatePhyoffset;
}
