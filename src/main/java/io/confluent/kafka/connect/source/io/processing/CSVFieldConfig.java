package io.confluent.kafka.connect.source.io.processing;

import com.google.common.base.Preconditions;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CSVFieldConfig extends AbstractConfig {

  static String[] types() {
    List<String> types = new ArrayList<>();
    for(Schema.Type type:Schema.Type.values()){
      types.add(type.name());
    }
    types.remove(Schema.Type.ARRAY.name());
    types.remove(Schema.Type.STRUCT.name());
    types.remove(Schema.Type.MAP.name());
    String[] typeArray = new String[types.size()];

    return types.toArray(typeArray);
  }

  static String[] logicalTypes() {
    List<String> types = new ArrayList<>();
    for(LogicalType logicalType:LogicalType.values()){
      types.add(logicalType.name());
    }
    String[] typeArray = new String[types.size()];

    return types.toArray(typeArray);
  }

  public void addField(SchemaBuilder builder) {
    Preconditions.checkState(Schema.Type.STRUCT == builder.type(), "Only structs are supported.");
    builder.field(name(), schema());
  }

  public enum LogicalType{
    none,
    decimal,
    time,
    timestamp,
    date
  }


  public static String NAME_CONF="name";
  static String NAME_DOC="Name of the field.";


  public static String SCHEMA_TYPE_CONF ="schema.type";
  static String SCHEMA_TYPE_DOC ="type";
  static String SCHEMA_TYPE_DEFAULT =ConfigDef.Type.STRING.name();
  static String[] SCHEMA_TYPE_VALID_VALUES = types();

  public static String LOGICAL_TYPE_CONF="logical.type";
  static String LOGICAL_TYPE_DOC="logical type.";
  static final String LOGICAL_TYPE_DEFAULT="none";
  static final String[] LOGICAL_TYPE_VALID=logicalTypes();

  public static String DECIMAL_SCALE_CONF="decimal.scale";
  static final String DECIMAL_SCALE_DOC="The scale value for a decimal.";
  static final Integer DECIMAL_SCALE_DEFAULT=10;

  public static String REQUIRED_CONF ="required";
  static final String REQUIRED_DOC ="Flag to determine if field is required and can accept nulls.";
  static boolean REQUIRED_DEFAULT=false;


  public static ConfigDef conf() {
    return new ConfigDef()
        .define(NAME_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, NAME_DOC)
        .define(SCHEMA_TYPE_CONF, ConfigDef.Type.STRING, SCHEMA_TYPE_DEFAULT, ConfigDef.ValidString.in(SCHEMA_TYPE_VALID_VALUES), ConfigDef.Importance.HIGH, SCHEMA_TYPE_DOC)
        .define(LOGICAL_TYPE_CONF, ConfigDef.Type.STRING, LOGICAL_TYPE_DEFAULT, ConfigDef.ValidString.in(LOGICAL_TYPE_VALID), ConfigDef.Importance.HIGH, LOGICAL_TYPE_DOC)
        .define(DECIMAL_SCALE_CONF, ConfigDef.Type.INT, DECIMAL_SCALE_DEFAULT, ConfigDef.Importance.HIGH, DECIMAL_SCALE_DOC)
        .define(REQUIRED_CONF, ConfigDef.Type.BOOLEAN, REQUIRED_DEFAULT, ConfigDef.Importance.HIGH, REQUIRED_DOC)
        ;
  }

  public CSVFieldConfig(Map<?, ?> originals) {
    super(conf(), originals);
  }
  
  public String name(){
    return this.getString(NAME_CONF);
  }
  
  public Schema.Type schemaType() {
    String value = this.getString(SCHEMA_TYPE_CONF);
    return Schema.Type.valueOf(value);
  }
  
  public LogicalType logicalType(){
    String value = this.getString(LOGICAL_TYPE_CONF);
    return LogicalType.valueOf(value);
  }

  public int decimalScale() {
    return this.getInt(DECIMAL_SCALE_CONF);
  }

  public boolean required(){
    return this.getBoolean(REQUIRED_CONF);
  }

  public Schema schema() {
    SchemaBuilder builder;

    switch (logicalType()){
      case decimal:
        builder = Decimal.builder(decimalScale());
        break;
      case timestamp:
        builder = Timestamp.builder();
        break;
      case date:
        builder = Date.builder();
        break;
      case time:
        builder = Time.builder();
        break;
      case none:
        builder = SchemaBuilder.type(schemaType());
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "LogicalType of %s is not supported.",
                logicalType()
            )
        );
    }

    if(!required()){
      builder = builder.optional();
    }

    return builder.build();
  }
  
}
