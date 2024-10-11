package com.mawen.learn.rocketmq.remoting;

import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public interface CommandCustomHeader {

	void checkFields() throws RemotingCommandException;
}
