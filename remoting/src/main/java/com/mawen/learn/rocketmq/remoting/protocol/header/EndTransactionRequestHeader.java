package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.common.resource.RocketMQResource;
import com.mawen.learn.rocketmq.common.sysflag.MessageSysFlag;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.annotation.CFNullable;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import com.mawen.learn.rocketmq.remoting.rpc.RpcRequestHeader;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@Getter
@Setter
@ToString
@RocketMQAction(value = RequestCode.END_TRANSACTION, action = Action.PUB)
public class EndTransactionRequestHeader extends RpcRequestHeader {

	@RocketMQResource(ResourceType.TOPIC)
	private String topic;

	@CFNotNull
	private String producerGroup;

	@CFNotNull
	private Long tranStateTableOffset;

	@CFNotNull
	private Long commitLogOffset;

	@CFNotNull
	private Integer commitOrRollback;

	@CFNullable
	private Boolean fromTransactionCheck = false;

	@CFNotNull
	private String msgId;

	private String transactionId;

	@Override
	public void checkFields() throws RemotingCommandException {
		if (MessageSysFlag.TRANSACTION_NOT_TYPE == this.commitOrRollback) {
			return;
		}

		if (MessageSysFlag.TRANSACTION_COMMIT_TYPE == this.commitOrRollback) {
			return;
		}

		if (MessageSysFlag.TRANSACTION_ROLLBACK_TYPE == this.commitOrRollback) {
			return;
		}

		throw new RemotingCommandException("commitOrRollback field wrong");
	}
}
