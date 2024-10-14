package com.mawen.learn.rocketmq.remoting.netty;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public interface RemotingResponseCallback {

	void callback(RemotingCommand response);
}
