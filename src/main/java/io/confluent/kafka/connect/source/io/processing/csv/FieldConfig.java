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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class FieldConfig implements Comparable<FieldConfig> {
  public String name;
  public FieldConfig.Type type;
  public Integer scale;
  public boolean required;

  private static int scale(Schema schema) {
    String scaleString = (String) schema.parameters().get("scale");
    if (scaleString == null) {
      throw new DataException("Invalid Decimal schemaConfig: scale parameter not found.");
    } else {
      try {
        return Integer.parseInt(scaleString);
      } catch (NumberFormatException var3) {
        throw new DataException("Invalid scale parameter found in Decimal schemaConfig: ", var3);
      }
    }
  }

  public static FieldConfig create(Schema schema) {
    Preconditions.checkNotNull(schema, "SchemaConfig cannot be null.");
    FieldConfig config = new FieldConfig();

    config.required = !schema.isOptional();

    if (schema.name() == Decimal.LOGICAL_NAME) {
      config.type = Type.DECIMAL;
      config.scale = scale(schema);
    } else if (schema.name() == Time.LOGICAL_NAME) {
      config.type = Type.TIME;
    } else if (schema.name() == Date.LOGICAL_NAME) {
      config.type = Type.DATE;
    } else if (schema.name() == Timestamp.LOGICAL_NAME) {
      config.type = Type.TIMESTAMP;
    } else {
      String schemaType = schema.type().name();
      FieldConfig.Type type = FieldConfig.Type.valueOf(schemaType);
      config.type = type;
    }

    return config;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        this.name,
        this.type,
        this.scale,
        this.required
    );
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", this.name)
        .add("type", this.type)
        .add("scale", this.scale)
        .add("required", this.required)
        .toString();
  }


  @Override
  public int compareTo(FieldConfig that) {
    if (null == that) {
      return 1;
    }
    return ComparisonChain.start()
        .compare(this.name, that.name, Ordering.natural().<String>nullsFirst())
        .compare(this.type, that.type)
        .compare(this.scale, that.scale, Ordering.natural().<Integer>nullsFirst())
        .compare(this.required, that.required)
        .result();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FieldConfig) {
      return compareTo((FieldConfig) obj) == 0;
    } else {
      return false;
    }
  }


  public enum Type {
    DECIMAL("decimal"),

    TIME("time"),

    TIMESTAMP("timestamp"),

    DATE("date"),

    INT8("int8"),

    INT16("int16"),

    INT32("int32"),

    INT64("int64"),

    FLOAT32("float32"),

    FLOAT64("float64"),

    BOOLEAN("boolean"),

    STRING("string"),
    BYTES("bytes");

    private final String value;

    Type(final String s) {
      this.value = s;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  public Schema schema() {
    SchemaBuilder builder;

    switch (this.type) {
      case TIME:
        builder = Time.builder();
        break;
      case TIMESTAMP:
        builder = Timestamp.builder();
        break;
      case DECIMAL:
        int scale = Preconditions.checkNotNull(this.scale, "scale must be set for decimals.");
        Preconditions.checkState(scale >= 0, "Scale %s is invalid. Must be greater than 0.", scale);
        builder = Decimal.builder(scale);
        break;
      case DATE:
        builder = Date.builder();
        break;
      default:
        Schema.Type schemaType = Schema.Type.valueOf(this.type.name());
        builder = SchemaBuilder.type(schemaType);
        break;
    }

    if (!this.required) {
      builder = builder.optional();
    }

    return builder.build();
  }
}
