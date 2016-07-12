/**
 * Copyright (C) 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.kafka.connect.source.io.processing.csv;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Test;

public class FieldConfigTest {

  void assertCreate(final Schema schema, final FieldConfig.Type expectedType) {
    FieldConfig expected = FieldConfig.create(schema);
    FieldConfig actual = FieldConfig.create(schema);
    Assert.assertThat(actual, IsEqual.equalTo(expected));
  }


  @Test
  public void createBoolean() {
    assertCreate(Schema.BOOLEAN_SCHEMA, FieldConfig.Type.BOOLEAN);
  }

  @Test
  public void createBytes() {
    assertCreate(Schema.BYTES_SCHEMA, FieldConfig.Type.BYTES);
  }

  @Test
  public void createDate() {
    assertCreate(Date.SCHEMA, FieldConfig.Type.DATE);
  }

  @Test
  public void createDecimal() {
    assertCreate(Decimal.schema(4), FieldConfig.Type.DECIMAL);
  }

  @Test
  public void createFloat32() {
    assertCreate(Schema.FLOAT32_SCHEMA, FieldConfig.Type.FLOAT32);
  }

  @Test
  public void createFloat64() {
    assertCreate(Schema.FLOAT64_SCHEMA, FieldConfig.Type.FLOAT64);
  }

  @Test
  public void createInt8() {
    assertCreate(Schema.INT8_SCHEMA, FieldConfig.Type.INT8);
  }

  @Test
  public void createInt16() {
    assertCreate(Schema.INT16_SCHEMA, FieldConfig.Type.INT16);
  }

  @Test
  public void createInt32() {
    assertCreate(Schema.INT32_SCHEMA, FieldConfig.Type.INT32);
  }

  @Test
  public void createInt64() {
    assertCreate(Schema.INT64_SCHEMA, FieldConfig.Type.INT64);
  }

  @Test
  public void createString() {
    assertCreate(Schema.STRING_SCHEMA, FieldConfig.Type.STRING);
  }

  @Test
  public void createTime() {
    assertCreate(Time.SCHEMA, FieldConfig.Type.TIME);
  }

  @Test
  public void createTimestamp() {
    assertCreate(Timestamp.SCHEMA, FieldConfig.Type.TIMESTAMP);
  }

  @Test
  public void createOptionalBoolean() {
    assertCreate(Schema.OPTIONAL_BOOLEAN_SCHEMA, FieldConfig.Type.BOOLEAN);
  }

  @Test
  public void createOptionalBytes() {
    assertCreate(Schema.OPTIONAL_BYTES_SCHEMA, FieldConfig.Type.BYTES);
  }

  @Test
  public void createOptionalDate() {
    assertCreate(Date.builder().optional().build(), FieldConfig.Type.DATE);
  }

  @Test
  public void createOptionalDecimal() {
    assertCreate(Decimal.builder(4).optional().build(), FieldConfig.Type.DECIMAL);
  }

  @Test
  public void createOptionalFloat32() {
    assertCreate(Schema.OPTIONAL_FLOAT32_SCHEMA, FieldConfig.Type.FLOAT32);
  }

  @Test
  public void createOptionalFloat64() {
    assertCreate(Schema.OPTIONAL_FLOAT64_SCHEMA, FieldConfig.Type.FLOAT64);
  }

  @Test
  public void createOptionalInt8() {
    assertCreate(Schema.OPTIONAL_INT8_SCHEMA, FieldConfig.Type.INT8);
  }

  @Test
  public void createOptionalInt16() {
    assertCreate(Schema.OPTIONAL_INT16_SCHEMA, FieldConfig.Type.INT16);
  }

  @Test
  public void createOptionalInt32() {
    assertCreate(Schema.OPTIONAL_INT32_SCHEMA, FieldConfig.Type.INT32);
  }

  @Test
  public void createOptionalInt64() {
    assertCreate(Schema.OPTIONAL_INT64_SCHEMA, FieldConfig.Type.INT64);
  }

  @Test
  public void createOptionalString() {
    assertCreate(Schema.OPTIONAL_STRING_SCHEMA, FieldConfig.Type.STRING);
  }

  @Test
  public void createOptionalTime() {
    assertCreate(Time.builder().optional().build(), FieldConfig.Type.TIME);
  }

  @Test
  public void createOptionalTimestamp() {
    assertCreate(Timestamp.builder().optional().build(), FieldConfig.Type.TIMESTAMP);
  }

  void assertSchema(FieldConfig config, Schema expected) {
    Schema actual = config.schema();
    Assert.assertThat(actual, IsEqual.equalTo(expected));
  }

  @Test
  public void toSchemaBoolean() {
    FieldConfig config = new FieldConfig();
    config.type = FieldConfig.Type.BOOLEAN;
    config.required = true;
    assertSchema(config, Schema.BOOLEAN_SCHEMA);
  }

  @Test
  public void createByCSVFieldConfig() {


  }

}
