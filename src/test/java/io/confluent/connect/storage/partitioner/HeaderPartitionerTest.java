package io.confluent.connect.storage.partitioner;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static io.confluent.connect.storage.partitioner.HeaderPartitioner.PARTITION_HEADER_NAME_CONFIG;
import static org.junit.jupiter.api.Assertions.*;

class HeaderPartitionerTest {

    @Test
    void shouldSetPartition() {
        HeaderPartitioner<String> partitioner = new HeaderPartitioner<>();

        Map<String, Object> configs = new HashMap<>();
        configs.put(PARTITION_HEADER_NAME_CONFIG, "a,b,c");
        configs.put("directory.delim", "/");
        partitioner.configure(configs);

        System.out.print(partitioner.delim);

//        SinkRecord record = new SinkRecord(
//                "test",
//                0,
//                Schema.BYTES_SCHEMA,
//                new byte[] {},
//                Schema.BOOLEAN_SCHEMA,
//                true,
//                0);
//        String partition = partitioner.encodePartition(record);

    }
}