package com.mawen.learn.rocketmq.client.trace.hook;

import com.mawen.learn.rocketmq.client.hook.EndTransactionContext;
import com.mawen.learn.rocketmq.client.hook.EndTransactionHook;
import com.mawen.learn.rocketmq.client.trace.TraceDispatcher;
import lombok.AllArgsConstructor;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/18
 */
@AllArgsConstructor
public class EndTransactionTraceHookImpl implements EndTransactionHook {

	private TraceDispatcher localDispatcher;

	@Override
	public String hookName() {
		return "EndTransactionTraceHook";
	}

	@Override
	public void endTransaction(EndTransactionContext context) {
		TODO
	}
}
