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

import io.confluent.kafka.connect.source.Data;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Test;

public class SchemaConfigTests {

  void assertSchema(Schema expected, Schema actual) {
    Assert.assertNotNull(actual);
    Assert.assertThat(actual.type(), IsEqual.equalTo(expected.type()));
    Assert.assertThat(actual.name(), IsEqual.equalTo(expected.name()));
//    Assert.assertThat(actual.type(), IsEqual.equalTo(Schema.Type.STRUCT));

    if (expected.type() == Schema.Type.STRUCT) {
      Assert.assertThat(actual.fields().size(), IsEqual.equalTo(expected.fields().size()));
      for (int i = 0; i < expected.fields().size(); i++) {
        Field expectedField = expected.fields().get(i);
        Field actualField = expected.fields().get(i);

        assertSchema(expectedField.schema(), actualField.schema());
        Assert.assertThat(expectedField.name(), IsEqual.equalTo(expectedField.name()));
        Assert.assertThat(expectedField.index(), IsEqual.equalTo(expectedField.index()));
      }
    }


  }

  @Test
  public void schema() {
    final SchemaConfig input = Data.getMockDataSchemaConfig();
    final Schema expected = SchemaBuilder.struct()
        .name("io.confluent.kafka.connect.source.MockData")
        .field("id", Schema.INT32_SCHEMA)
        .field("first_name", Schema.STRING_SCHEMA)
        .field("last_name", Schema.STRING_SCHEMA)
        .field("email", Schema.STRING_SCHEMA)
        .field("gender", Schema.STRING_SCHEMA)
        .field("ip_address", Schema.STRING_SCHEMA)
        .field("last_login", Timestamp.builder().optional())
        .field("account_balance", Decimal.builder(10).optional())
        .field("country", Schema.STRING_SCHEMA)
        .field("favorite_color", Schema.OPTIONAL_STRING_SCHEMA)
        .build();
    final Schema actual = input.schema();

    assertSchema(expected, actual);
  }


}
