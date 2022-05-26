/*
 * Copyright © 2019 Christopher Matta (chris.matta@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.sidnvy.kafka.connect.smt;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class InfluxdbLineProtocol<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String OVERVIEW_DOC =
    "Convert JSON record to conform with influxdb line protocol text";

  private interface ConfigName {
    String TIMESTAMP_NAME = "timestamp.names";
    String TAG_SET_NAMES = "tag.names";
    String FIELD_SET_NAMES = "field.names";
    String TOPIC_DELIMITER = "topic.delimiter.name";
  }

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(ConfigName.TIMESTAMP_NAME, ConfigDef.Type.STRING, "timestamp", ConfigDef.Importance.HIGH,
      "Field name for TIMESTAMP")
    .define(ConfigName.TAG_SET_NAMES, ConfigDef.Type.STRING, "exchange,symbol", ConfigDef.Importance.HIGH,
      "Field name for tag set")
    .define(ConfigName.FIELD_SET_NAMES, ConfigDef.Type.STRING, "amount,price,bid,ask", ConfigDef.Importance.HIGH,
      "Field name for field set")
    .define(ConfigName.TOPIC_DELIMITER, ConfigDef.Type.STRING, "-", ConfigDef.Importance.HIGH,
      "Field name for topic delimiter");

  private static final String PURPOSE = "select timestamp";

  private String[] timestampFieldNames;
  private String[] tagNames;
  private String[] fieldNames;
  private String topicDelimiter;


  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    timestampFieldNames = config.getString(ConfigName.TIMESTAMP_NAME).split(",");
    tagNames = config.getString(ConfigName.TAG_SET_NAMES).split(",");
    fieldNames = config.getString(ConfigName.FIELD_SET_NAMES).split(",");
    topicDelimiter = config.getString(ConfigName.TOPIC_DELIMITER);
  }


  @Override
  public R apply(R record) {
    if (operatingSchema(record) == null) {
      return applySchemaless(record);
    } else {
      return record;
    }
  }

  private R applySchemaless(R record) {
    final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
    final String line = convertMapToLine(record.topic(), value);
    return newRecord(record, null, line);
  }

  private String convertMapToLine(String topic, Map<String, Object> value) {
    String measurement = topic.split(topicDelimiter)[0];
    Map<String, Object> tagsSet = new HashMap<>();
    Map<String, Object> fieldSet = new HashMap<>();
    Map<String, Object> timestampSet = new HashMap<>();

    for (String key: value.keySet()) {
      if (Arrays.stream(tagNames).anyMatch(key::equals)) {
        tagsSet.put(key, value.get(key));
      } else if (Arrays.stream(fieldNames).anyMatch(key::equals)) {
        fieldSet.put(key, value.get(key));
      }
      if (Arrays.stream(timestampFieldNames).anyMatch(key::equals)) {
        timestampSet.put(key, value.get(key));
      }
    }

    StringBuilder base = new StringBuilder();
    for (String key: timestampSet.keySet()) {
      if (key == "receipt_timestamp") {
        base.append(String.format("%s,%s %s %s\n", measurement, mapToString(tagsSet), mapToString(fieldSet), secondsToMillisecond(timestampSet.get(key))));
      } else {
        base.append(String.format("%s,%s %s %s\n", measurement.concat("_").concat("origin"), mapToString(tagsSet), mapToString(fieldSet), secondsToMillisecond(timestampSet.get(key))));
      }
    }
    return base.toString();
  }

  private Long secondsToMillisecond(Object second) {
    if (second instanceof Double) {
        return (long) (((Double) second) * 1000);
    } else if (second instanceof Integer) {
        return Long.valueOf(((Integer) second).longValue()) * 1000;
    } else if (second instanceof Long) {
        return ((long) second) * 1000;
    } else {
      throw new DataException("Expected timestamp to be a Integer or Double, but found " + second.getClass());
    }
  }

  private String mapToString(Map<String, ?> map) {
    String mapAsString = map.keySet().stream()
      .map(key -> key + "=" + map.get(key))
      .collect(Collectors.joining(","));
    return mapAsString;
}

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class Key<R extends ConnectRecord<R>> extends InfluxdbLineProtocol<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), updatedValue, record.valueSchema(), record.value(), record.timestamp());
    }

  }

  public static class Value<R extends ConnectRecord<R>> extends InfluxdbLineProtocol<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }

  }
}


