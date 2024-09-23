package com.mawen.learn.rocketmq.common.action;

import com.alibaba.fastjson2.annotation.JSONField;
import org.apache.commons.lang3.StringUtils;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/22
 */
public enum Action {
	ANY((byte) 2, "Any"),
	PUB((byte) 3, "Pub"),
	SUB((byte) 4, "Sub"),
	CREATE((byte) 5, "Create"),
	UPDATE((byte) 6, "Update"),
	DELETE((byte) 7, "Delete"),
	GET((byte) 8, "Get"),
	LIST((byte) 9, "List");

	@JSONField(value = true)
	private final byte code;
	private final String name;

	Action(byte code, String name) {
		this.code = code;
		this.name = name;
	}

	public static Action getByName(String name) {
		for (Action action : Action.values()) {
			if (StringUtils.equalsIgnoreCase(action.getName(), name)) {
				return action;
			}
		}
		return null;
	}

	public byte getCode() {
		return code;
	}

	public String getName() {
		return name;
	}
}
