package com.mawen.learn.rocketmq4.spring.repository.elasticsearch;

import com.mawen.learn.rocketmq4.spring.entity.AssetIncDocument;

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/28
 */
@Repository
public interface AssetIncRepository extends ElasticsearchRepository<AssetIncDocument, String> {
}
