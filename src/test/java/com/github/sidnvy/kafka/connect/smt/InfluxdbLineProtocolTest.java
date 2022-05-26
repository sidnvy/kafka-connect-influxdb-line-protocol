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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class InfluxdbLineProtocolTest {

  private InfluxdbLineProtocol<SourceRecord> xform = new InfluxdbLineProtocol.Value<>();

  @After
  public void tearDown() throws Exception {
    xform.close();
  }


  @Test
  public void schemalessInsertUuidField() {
    final Map<String, Object> props = new HashMap<>();

    props.put("timestamp.names", "timestamp,receipt_timestamp");
    props.put("tag.names", "exchange,symbol");
    props.put("field.names", "price,amount,timestamp");
    // props.put("topic.delimiter.name", "\\.");

    xform.configure(props);
    final Map<String, Object> msg = new HashMap<String, Object>() {{
        put("exchange", "ZB");
        put("symbol", "QNT-USDC");
        put("side", "sell");
        put("amount", "1.6666");
        put("price", "65.7234");
        put("id", "7734503");
        put("type", null);
        put("timestamp", 1653480224);
        put("receipt_timestamp", 1653480225.7766595);
    }};

    final SourceRecord record = new SourceRecord(null, null, "trades-ZB", 0,
      null, msg);

    final SourceRecord transformedRecord = xform.apply(record);
    String[] splitLines = String.valueOf(transformedRecord.value()).split("\n");
    assertEquals("trades,symbol=QNT-USDC,exchange=ZB amount=1.6666,price=65.7234,timestamp=1653480224 1653480225776", splitLines[0]);
    assertEquals("trades_origin,symbol=QNT-USDC,exchange=ZB amount=1.6666,price=65.7234,timestamp=1653480224 1653480224000", splitLines[1]);
  }
}
