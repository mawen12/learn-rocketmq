package com.mawen.learn.rocketmq.store.timer;

/**
 * Represents a slot of timing wheel. Format:
 *
 * <pre>
 * +--------------+-----------+----------+---------+--------+
 * | delayed time | first pos | last pos |  num    | magic  |
 * +--------------+-----------+----------+---------+--------+
 * | 8bytes       | 8bytes    | 8bytes   | 4bytes  | 4bytes |
 * +--------------+-----------+----------+---------+--------+
 * </pre>
 *
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/5
 */
public class Slot {

	public static final short SIZE = 32;

	public final long timeMs;
	public final long firstPos;
	public final long lastPos;
	public final int num;
	public final int magic;

	public Slot(long timeMs, long firstPos, long lastPos) {
		this.timeMs = timeMs;
		this.firstPos = firstPos;
		this.lastPos = lastPos;
		this.num = 0;
		this.magic = 0;
	}

	public Slot(long timeMs, long firstPos, long lastPos, int num, int magic) {
		this.timeMs = timeMs;
		this.firstPos = firstPos;
		this.lastPos = lastPos;
		this.num = num;
		this.magic = magic;
	}
}
