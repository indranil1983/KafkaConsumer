package org.chandra;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.chandra.avro.ItemMsg;
import org.chandra.entity.CallDataRecord;
import org.chandra.repository.CallDataRecordRepository;
import org.chandra.monitor.KafkaLagMonitor; // Import the new monitor class
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class ConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerService.class);
    private final Schema schema;
    private final CallDataRecordRepository repository;
    private final AtomicLong processedMessageCount = new AtomicLong(0);
    private long lastLogTime = System.currentTimeMillis();
    private final KafkaLagMonitor kafkaLagMonitor; // Inject the new monitor

    public ConsumerService(CallDataRecordRepository repository, KafkaLagMonitor kafkaLagMonitor) throws IOException {
        this.repository = repository;
        this.kafkaLagMonitor = kafkaLagMonitor; // Initialize the monitor
        try (InputStream is = getClass().getResourceAsStream("/avro/item.avsc")) {
            if (is == null) {
                throw new IOException("Avro schema file not found in resources");
            }
            this.schema = new Schema.Parser().parse(is);
        }
    }

    @Transactional
    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}", id = "itemConsumerListener")
    public void consume(List<byte[]> dataList, Acknowledgment acknowledgment) {
        
        logger.debug("Received batch of {} messages from Kafka", dataList.size());
        List<CallDataRecord> recordsToSave = new ArrayList<>();

        for (byte[] data : dataList) {
            try {
                ItemMsg item = deserializeAvro(data);
                CallDataRecord record = mapToEntity(item);
                recordsToSave.add(record);
            } catch (IOException e) {
                logger.error("Error deserializing message in batch. Skipping individual message.", e);
            }
        }

        if (!recordsToSave.isEmpty()) {
            repository.saveAll(recordsToSave);
            logger.debug("Successfully saved batch of {} records to database", recordsToSave.size());
            processedMessageCount.addAndGet(recordsToSave.size()); // Increment count for successfully saved records
        }

        acknowledgment.acknowledge();
        logger.debug("Batch offset committed manually");
    }

    private CallDataRecord mapToEntity(ItemMsg item) {
        CallDataRecord record = new CallDataRecord();
        record.setUlid(item.getUlid().toString());
        record.setName(item.getName().toString());
        record.setPrice(item.getPrice());
        record.setQuantity(item.getQuantity());
        return record;
    }

    private ItemMsg deserializeAvro(byte[] data) throws IOException {
        DatumReader<ItemMsg> reader = new SpecificDatumReader<>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        return reader.read(null, decoder);
    }

    @Scheduled(fixedRate = 30000) // Every 30 seconds
    public void logThroughputAndLag() {
        // Log Throughput
        long currentCount = processedMessageCount.getAndSet(0); // Get current count and reset to 0
        long currentTime = System.currentTimeMillis();
        double durationSeconds = (currentTime - lastLogTime) / 1000.0;
        lastLogTime = currentTime;

        if (durationSeconds > 0) {
            double throughput = currentCount / durationSeconds;
            logger.info("Throughput: {} messages/second ({} messages in {} seconds)", 
                        String.format("%.2f", throughput), currentCount, String.format("%.2f", durationSeconds));
        } else {
            logger.info("Throughput: 0 messages/second (no messages processed in the last interval)");
        }

        // Log Topic Lag
        kafkaLagMonitor.logKafkaLag("itemConsumerListener"); // Delegate to the new monitor class
    }
}
