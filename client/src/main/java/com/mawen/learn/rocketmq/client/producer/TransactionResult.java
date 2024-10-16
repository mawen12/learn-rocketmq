package com.mawen.learn.rocketmq.client.producer;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
@NoArgsConstructor
@Setter
@Getter
public class TransactionResult extends SendResult{

	private LocalTransactionState localTransactionState;
}
