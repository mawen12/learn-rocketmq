package com.mawen.learn.rocketmq.common;

import java.io.Serializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class Pair<T1, T2> implements Serializable {

	private T1 object1;
	private T2 object2;

	public Pair(T1 object1, T2 object2) {
		this.object1 = object1;
		this.object2 = object2;
	}

	public static <T1, T2> Pair<T1, T2> of(T1 object1, T2 object2) {
		return new Pair<>(object1, object2);
	}

	public T1 getObject1() {
		return object1;
	}

	public void setObject1(T1 object1) {
		this.object1 = object1;
	}

	public T2 getObject2() {
		return object2;
	}

	public void setObject2(T2 object2) {
		this.object2 = object2;
	}
}
