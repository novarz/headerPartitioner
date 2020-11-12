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
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class HeaderPartitioner<T> extends DefaultPartitioner<T> {
    private static final Logger LOG = LoggerFactory.getLogger(HeaderPartitioner.class);

    static final String PARTITION_HEADER_NAME_CONFIG = "partition.header.name";

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("'year='yyyy'/month='MM'/day='dd");

    private List<String> headerNames;

    @Override
    public void configure(Map<String, Object> config) {
        super.configure(config);
        String headersRaw = (String) config.get(PARTITION_HEADER_NAME_CONFIG);
        headerNames = Arrays.asList(headersRaw.split(",", -1));
        if (this.delim == null) {
            LOG.warn("directory.delim not set, using / as default.");
            this.delim = "/";
        }
    }

    @Override
    public String encodePartition(SinkRecord sinkRecord) {
        Headers headers = sinkRecord.headers();
        StringBuilder builder = new StringBuilder();
        for (String headerName : headerNames) {
            if (builder.length() > 0) {
                builder.append(this.delim);
            }
            LOG.debug(headerName);
            Header partitionKey = headers.lastWithName(headerName);
            if (partitionKey.value() == null) {
                builder.append(headerName).append("=");
            } else {
                builder.append(headerName).append("=").append(partitionKey.value().toString().replace(' ', '_'));
                // failure
            }

        }
        builder.append(this.delim);
        builder.append(LocalDate.now().format(FORMATTER));

        return builder.toString();
    }

    @Override
    public List<T> partitionFields() {
        if (partitionFields == null) {
            partitionFields = newSchemaGenerator(config)
                    .newPartitionFields(Utils.join(headerNames, ","));
        }
        return partitionFields;
    }
}