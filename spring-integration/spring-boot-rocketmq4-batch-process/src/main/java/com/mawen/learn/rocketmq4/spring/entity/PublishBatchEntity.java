package com.mawen.learn.rocketmq4.spring.entity;

import java.util.Date;

import com.mawen.learn.rocketmq4.spring.enums.FlowState;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/27
 */
@Data
@Entity
@Table(name = "pub_batch")
public class PublishBatchEntity {

	@Id
	@Column(name = "batch_id")
	private Long id;

	@Column(name = "batch_nm")
	private String name;

	@Column(name = "flow_state")
	private FlowState flowState;

	@Column(name = "crt_usr_id")
	private Long createUserId;

	@Column(name = "crt_tm")
	private Date createTime;

	@Column(name = "latest_update_usr_id")
	private Long latestUpdateUserId;

	@Column(name = "latest_update_tm")
	private Date latestUpdateTime;
}
