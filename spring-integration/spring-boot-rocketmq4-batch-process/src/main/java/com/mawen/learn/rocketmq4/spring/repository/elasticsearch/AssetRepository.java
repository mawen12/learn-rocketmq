package com.mawen.learn.rocketmq4.spring.repository.elasticsearch;

import java.util.stream.Stream;

import com.mawen.learn.rocketmq4.spring.entity.AssetDocument;
import com.mawen.learn.rocketmq4.spring.enums.PublishState;

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/27
 */
@Repository
public interface AssetRepository extends ElasticsearchRepository<AssetDocument, String> {

	Stream<AssetDocument> streamByPublishState(PublishState state);

	int countByPublishState(PublishState state);
}
