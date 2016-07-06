package io.confluent.kafka.connect.source;

import io.confluent.kafka.connect.source.io.processing.CSVFieldConfig;
import org.apache.kafka.connect.data.Schema;

import java.io.InputStream;
import java.util.Map;
import java.util.TreeMap;

public class Data {

  public static InputStream getMockData() {
    return Data.class.getResourceAsStream("MOCK_DATA.csv.old");
  }

  static void add(Map<String, String> settings, Integer index, String key, String value) {
    String mapKey = String.format("fields.%02d.%s", index, key);
    settings.put(mapKey, value);
  }

  public static Map<String, String> getMockDataSettings() {
    int index = 0;
    Map<String, String> settings = new TreeMap<>();

//    id,first_name,last_name,email,gender,ip_address,last_login,account_balance,country,favorite_color
//    1,Jack,Garcia,jgarcia0@shop-pro.jp,Male,196.56.44.185,2015-09-30T15:29:03Z,$347.77,IT,#4a2313

    add(settings, index, CSVFieldConfig.NAME_CONF, "id");
    add(settings, index, CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.INT32.name());
    add(settings, index, CSVFieldConfig.REQUIRED_CONF, Boolean.TRUE.toString());
    index++;

    add(settings, index, CSVFieldConfig.NAME_CONF, "first_name");
    add(settings, index, CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.STRING.name());
    add(settings, index, CSVFieldConfig.REQUIRED_CONF, Boolean.TRUE.toString());
    index++;

    add(settings, index, CSVFieldConfig.NAME_CONF, "last_name");
    add(settings, index, CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.STRING.name());
    add(settings, index, CSVFieldConfig.REQUIRED_CONF, Boolean.TRUE.toString());
    index++;

    add(settings, index, CSVFieldConfig.NAME_CONF, "title");
    add(settings, index, CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.STRING.name());
    add(settings, index, CSVFieldConfig.REQUIRED_CONF, Boolean.TRUE.toString());
    index++;

    add(settings, index, CSVFieldConfig.NAME_CONF, "email");
    add(settings, index, CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.STRING.name());
    add(settings, index, CSVFieldConfig.REQUIRED_CONF, Boolean.TRUE.toString());
    index++;

    add(settings, index, CSVFieldConfig.NAME_CONF, "gender");
    add(settings, index, CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.STRING.name());
    add(settings, index, CSVFieldConfig.REQUIRED_CONF, Boolean.TRUE.toString());
    index++;

    add(settings, index, CSVFieldConfig.NAME_CONF, "ip_address");
    add(settings, index, CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.STRING.name());
    add(settings, index, CSVFieldConfig.REQUIRED_CONF, Boolean.TRUE.toString());
    index++;

    add(settings, index, CSVFieldConfig.NAME_CONF, "last_login");
    add(settings, index, CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.STRING.name());
    add(settings, index, CSVFieldConfig.LOGICAL_TYPE_CONF, CSVFieldConfig.LogicalType.timestamp.toString());
    add(settings, index, CSVFieldConfig.REQUIRED_CONF, Boolean.FALSE.toString());
    index++;

    add(settings, index, CSVFieldConfig.NAME_CONF, "account_balance");
    add(settings, index, CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.STRING.name());
    add(settings, index, CSVFieldConfig.LOGICAL_TYPE_CONF, CSVFieldConfig.LogicalType.decimal.toString());
    add(settings, index, CSVFieldConfig.REQUIRED_CONF, Boolean.FALSE.toString());
    index++;

    add(settings, index, CSVFieldConfig.NAME_CONF, "country");
    add(settings, index, CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.STRING.name());
    add(settings, index, CSVFieldConfig.REQUIRED_CONF, Boolean.TRUE.toString());
    index++;

    add(settings, index, CSVFieldConfig.NAME_CONF, "favorite_color");
    add(settings, index, CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.STRING.name());
    add(settings, index, CSVFieldConfig.REQUIRED_CONF, Boolean.FALSE.toString());
    index++;

    return settings;
  }

}
