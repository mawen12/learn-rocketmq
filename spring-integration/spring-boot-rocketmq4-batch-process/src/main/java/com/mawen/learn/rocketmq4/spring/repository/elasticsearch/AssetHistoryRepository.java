package com.mawen.learn.rocketmq4.spring.repository.elasticsearch;

import com.mawen.learn.rocketmq4.spring.entity.AssetHistoryDocument;

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/27
 */
@Repository
public interface AssetHistoryRepository extends ElasticsearchRepository<AssetHistoryDocument, String> {
}
