package com.mawen.learn.rocketmq.client.trace;

import java.util.HashSet;
import java.util.Set;

import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/18
 */
@Getter
@Setter
public class TraceTransferBean {

	private String transData;

	private Set<String> transKey = new HashSet<>();
}
