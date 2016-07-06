package io.confluent.kafka.connect.source.io.processing;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class CSVFieldConfigTests {

  @Test
  public void dumpConfig() {
    System.out.println(CSVFieldConfig.conf().toRst());
  }

  private Schema assertSchema(Map<?, ?> settings, Schema expectedSchema) {
    CSVFieldConfig config = new CSVFieldConfig(settings, 0);
    final Schema actualSchema = config.schema();

    Assert.assertThat("name should match", actualSchema.name(), IsEqual.equalTo(expectedSchema.name()));
    Assert.assertThat("type should match", actualSchema.type(), IsEqual.equalTo(expectedSchema.type()));
    Assert.assertThat("isOptional should match", actualSchema.isOptional(), IsEqual.equalTo(expectedSchema.isOptional()));
    Assert.assertThat("parameters do not match", actualSchema.parameters(), IsEqual.equalTo(expectedSchema.parameters()));
    return actualSchema;
  }

  @Test
  public void schemaString() {
    Map<?, ?> settings = ImmutableMap.of(
        CSVFieldConfig.NAME_CONF, "fieldName",
        CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.STRING.name()
    );
    assertSchema(settings, Schema.OPTIONAL_STRING_SCHEMA);
  }

  @Test
  public void schemaBoolean() {
    Map<?, ?> settings = ImmutableMap.of(
        CSVFieldConfig.NAME_CONF, "fieldName",
        CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.BOOLEAN.name()
    );
    assertSchema(settings, Schema.OPTIONAL_BOOLEAN_SCHEMA);
  }

  @Test
  public void schemaFloat32() {
    Map<?, ?> settings = ImmutableMap.of(
        CSVFieldConfig.NAME_CONF, "fieldName",
        CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.FLOAT32.name()
    );
    assertSchema(settings, Schema.OPTIONAL_FLOAT32_SCHEMA);
  }

  @Test
  public void schemaFloat64() {
    Map<?, ?> settings = ImmutableMap.of(
        CSVFieldConfig.NAME_CONF, "fieldName",
        CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.FLOAT64.name()
    );
    assertSchema(settings, Schema.OPTIONAL_FLOAT64_SCHEMA);
  }

  @Test
  public void schemaInt8() {
    Map<?, ?> settings = ImmutableMap.of(
        CSVFieldConfig.NAME_CONF, "fieldName",
        CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.INT8.name()
    );
    assertSchema(settings, Schema.OPTIONAL_INT8_SCHEMA);
  }

  @Test
  public void schemaInt16() {
    Map<?, ?> settings = ImmutableMap.of(
        CSVFieldConfig.NAME_CONF, "fieldName",
        CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.INT16.name()
    );
    assertSchema(settings, Schema.OPTIONAL_INT16_SCHEMA);
  }

  @Test
  public void schemaInt32() {
    Map<?, ?> settings = ImmutableMap.of(
        CSVFieldConfig.NAME_CONF, "fieldName",
        CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.INT32.name()
    );
    assertSchema(settings, Schema.OPTIONAL_INT32_SCHEMA);
  }

  @Test
  public void schemaInt64() {
    Map<?, ?> settings = ImmutableMap.of(
        CSVFieldConfig.NAME_CONF, "fieldName",
        CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.INT64.name()
    );
    assertSchema(settings, Schema.OPTIONAL_INT64_SCHEMA);
  }

  @Test
  public void schemaDecimal() {
    Map<?, ?> settings = ImmutableMap.of(
        CSVFieldConfig.NAME_CONF, "fieldName",
        CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.INT64.name(),
        CSVFieldConfig.LOGICAL_TYPE_CONF, CSVFieldConfig.LogicalType.decimal.name()
    );
    assertSchema(settings, Decimal.builder(CSVFieldConfig.DECIMAL_SCALE_DEFAULT).optional().build());
  }

  @Test
  public void field() {
    final String fieldName = "fieldName";
    Map<?, ?> settings = ImmutableMap.of(
        CSVFieldConfig.NAME_CONF, fieldName,
        CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.INT64.name(),
        CSVFieldConfig.LOGICAL_TYPE_CONF, CSVFieldConfig.LogicalType.decimal.name()
    );
    CSVFieldConfig config = new CSVFieldConfig(settings, 0);
    SchemaBuilder builder = SchemaBuilder.struct();
    config.addField(builder);
    Field field = builder.field(fieldName);
    Assert.assertNotNull(field);
    Assert.assertThat(fieldName, IsEqual.equalTo(field.name()));


  }

}
