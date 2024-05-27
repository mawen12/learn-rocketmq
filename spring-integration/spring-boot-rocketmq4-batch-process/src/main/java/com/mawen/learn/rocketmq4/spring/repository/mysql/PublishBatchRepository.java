package com.mawen.learn.rocketmq4.spring.repository.mysql;

import com.mawen.learn.rocketmq4.spring.entity.PublishBatchEntity;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/27
 */
@Repository
public interface PublishBatchRepository extends CrudRepository<PublishBatchEntity, Long> {
}
