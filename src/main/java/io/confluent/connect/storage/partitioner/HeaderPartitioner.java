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
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import java.util.*;

import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.errors.PartitionException;

public class HeaderPartitioner<T> extends DefaultPartitioner<T> {
    private static final Logger log = LoggerFactory.getLogger(HeaderPartitioner.class);
    public static final String PARTITION_HEADER_NAME_CONFIG = "partition.header.name";
  //  public static final String PARTITION_HEADER_FORMATTER_PATTERN= "partition.header.pattern";
    //  public static final String PARTITION_HEADER_FORMATTER_TimeZone= "partition.header.timezone";
    //  public static final String PARTITION_HEADER_FORMATTER_TimeZone= "partition.header.locale";



    private List<String> headerNames;
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("'year='yyyy'/month='MM'/day='dd");




    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, Object> config) {
        String headersRaw = (String)config.get(PARTITION_HEADER_NAME_CONFIG);
        log.debug ("headers " + headersRaw);
        headerNames = (List<String>) Arrays.asList(headersRaw.split(",", -1));
        delim = (String) config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
    }

    @Override
    public String encodePartition(SinkRecord sinkRecord) {

        Headers headers = sinkRecord.headers();
        log.debug("Lista total" + headerNames.size());

        for (String s : headerNames) {
            log.debug(s);

        }
            StringBuilder builder = new StringBuilder();
            for (String headerName : headerNames) {
                if (builder.length() > 0) {
                    builder.append(this.delim);
                }
                log.debug(headerName);
                Header partitionKey = headers.lastWithName(headerName);
                if (partitionKey.value() == null ) {
                    builder.append(headerName + "=" + "");
                }
                else{
                    builder.append(headerName + "=" + partitionKey.value().toString().replace(' ', '_'));
                    // failure
            }

            }
                builder.append(this.delim);
                builder.append(LocalDate.now().format(formatter));


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