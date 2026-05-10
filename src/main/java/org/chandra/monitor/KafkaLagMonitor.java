package org.chandra.monitor;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Component
public class KafkaLagMonitor {

    private static final Logger logger = LoggerFactory.getLogger(KafkaLagMonitor.class);
    private final AdminClient adminClient;
    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    public KafkaLagMonitor(KafkaAdmin kafkaAdmin, KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry) {
        this.adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
    }

    public void logKafkaLag(String listenerId) {
        try {
            MessageListenerContainer container = kafkaListenerEndpointRegistry.getListenerContainer(listenerId);
            if (container == null || !container.isRunning()) {
                logger.debug("Kafka listener container with id '{}' is not running or not found.", listenerId);
                return;
            }

            String topic = container.getContainerProperties().getTopics()[0]; // Assuming one topic
            String groupId = container.getGroupId();

            if (topic == null || groupId == null) {
                logger.warn("Could not determine topic or group ID for lag calculation for listener '{}'.", listenerId);
                return;
            }

            // Get topic partitions
            Set<TopicPartition> topicPartitions = adminClient.describeTopics(Collections.singleton(topic))
                    .all().get()
                    .values().stream()
                    .flatMap(td -> td.partitions().stream()
                            .map(pi -> new TopicPartition(td.name(), pi.partition())))
                    .collect(Collectors.toSet());

            if (topicPartitions.isEmpty()) {
                logger.warn("No partitions found for topic: {}", topic);
                return;
            }

            // Get end offsets (latest offsets)
            Map<TopicPartition, Long> endOffsets = adminClient.listOffsets(
                    topicPartitions.stream().collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest())))
                    .all().get()
                    .entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));

            // Get consumer group offsets
            ListConsumerGroupOffsetsOptions options = new ListConsumerGroupOffsetsOptions().topicPartitions(new ArrayList<>(topicPartitions));
            ListConsumerGroupOffsetsResult consumerGroupOffsetsResult = adminClient.listConsumerGroupOffsets(groupId, options);
            Map<TopicPartition, Long> consumerOffsets = consumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));

            long totalLag = 0;
            for (TopicPartition tp : topicPartitions) {
                Long consumerOffset = consumerOffsets.getOrDefault(tp, 0L);
                Long endOffset = endOffsets.getOrDefault(tp, 0L);
                long partitionLag = endOffset - consumerOffset;
                totalLag += partitionLag;
                logger.debug("Topic: {}, Partition: {}, Consumer Offset: {}, End Offset: {}, Lag: {}",
                        tp.topic(), tp.partition(), consumerOffset, endOffset, partitionLag);
            }
            logger.info("Kafka Lag for Group '{}' on Topic '{}': {} messages", groupId, topic, totalLag);

        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error calculating Kafka lag: {}", e.getMessage());
        } catch (Exception e) {
            logger.error("Unexpected error in KafkaLagMonitor: {}", e.getMessage(), e);
        }
    }
}
