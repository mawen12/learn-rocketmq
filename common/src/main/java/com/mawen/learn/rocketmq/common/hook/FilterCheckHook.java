package com.mawen.learn.rocketmq.common.hook;

import java.nio.ByteBuffer;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/9
 */
public interface FilterCheckHook {

	String hookName();

	boolean isFilterMatched(final boolean isUnitMode, final ByteBuffer byteBuffer);
}
