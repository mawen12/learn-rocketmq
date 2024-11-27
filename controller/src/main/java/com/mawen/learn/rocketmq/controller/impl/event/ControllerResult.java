package com.mawen.learn.rocketmq.controller.impl.event;

import java.util.ArrayList;
import java.util.List;

import com.mawen.learn.rocketmq.remoting.protocol.ResponseCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/26
 */
@ToString
public class ControllerResult<T> {

	private final List<EventMessage> events;
	@Getter
	private final T response;
	@Getter
	@Setter
	private byte[] body;
	@Getter
	private int responseCode = ResponseCode.SUCCESS;
	@Getter
	@Setter
	private String remark;

	public ControllerResult() {
		this(null);
	}

	public ControllerResult(T response) {
		this.events = new ArrayList<>();
		this.response = response;
	}

	public ControllerResult(List<EventMessage> events, T response) {
		this.events = new ArrayList<>(events);
		this.response = response;
	}

	public static <T> ControllerResult<T> of(List<EventMessage> events, T response) {
		return new ControllerResult<>(events, response);
	}

	public List<EventMessage> getEvents() {
		return new ArrayList<>(events);
	}

	public void setCodeAndRemark(int responseCode, String remark) {
		this.responseCode = responseCode;
		this.remark = remark;
	}

	public void addEvent(EventMessage event) {
		this.events.add(event);
	}
}
