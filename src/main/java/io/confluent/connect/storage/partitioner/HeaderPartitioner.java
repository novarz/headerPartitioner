/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.storage.partitioner;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.errors.PartitionException;

public class HeaderPartitioner<T> extends DefaultPartitioner<T> {
    private static final Logger log = LoggerFactory.getLogger(HeaderPartitioner.class);
    public static final String PARTITION_HEADER_NAME_CONFIG = "partition.header.name";
    private List<String> headerNames;

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, Object> config) {
        headerNames = (List<String>) config.get(PARTITION_HEADER_NAME_CONFIG);
        delim = (String) config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
    }

    @Override
    public String encodePartition(SinkRecord sinkRecord) {

        Headers headers = sinkRecord.headers();

            StringBuilder builder = new StringBuilder();
        for (String headerName : headerNames) {
            if (builder.length() > 0) {
                builder.append(this.delim);
            }
            Object partitionKey = headers.lastWithName(headerName);
            Type type = headers.lastWithName(headerName).schema().type();
            switch (type) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                    Number record = (Number) partitionKey;
                    builder.append(headerName + "=" + record.toString());
                    break;
                case STRING:
                    builder.append(headerName + "=" + (String) partitionKey);
                    break;
                case BOOLEAN:
                    boolean booleanRecord = (boolean) partitionKey;
                    builder.append(headerName + "=" + Boolean.toString(booleanRecord));
                    break;
                default:
                    log.error("Type {} is not supported as a partition key.", type.getName());
                    throw new PartitionException("Error encoding partition.");
            }
        }
            return builder.toString();
        }

    @Override
    public List<T> partitionFields() {
        if (partitionFields == null) {
            partitionFields = newSchemaGenerator(config).newPartitionFields(
                    Utils.join(headerNames, ",")
            );
        }
        return partitionFields;
    }
}