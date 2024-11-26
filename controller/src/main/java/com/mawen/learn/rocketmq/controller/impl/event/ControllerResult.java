package com.mawen.learn.rocketmq.controller.impl.event;

import java.util.ArrayList;
import java.util.List;

import com.mawen.learn.rocketmq.remoting.protocol.ResponseCode;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/26
 */
public class ControllerResult<T> {

	private final List<EventMessage> events;
	private final T response;
	private byte[] body;
	private int responseCode = ResponseCode.SUCCESS;
	private String remark;

	public ControllerResult() {
		this(null);
	}

	public ControllerResult(T response) {
		this.events = new ArrayList<>();
		this.response = response;
	}

	public ControllerResult() {

	}
}
