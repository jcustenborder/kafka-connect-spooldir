package io.confluent.kafka.connect.source;

import io.confluent.kafka.connect.source.io.processing.CSVFieldConfig;
import org.apache.kafka.connect.data.Schema;

import java.io.InputStream;
import java.util.Map;
import java.util.TreeMap;

public class Data {

  public static InputStream getMockData() {
    return Data.class.getResourceAsStream("MOCK_DATA.csv");
  }

  public static Map<String, String> getMockDataSettings() {
    int index = 0;
    Map<String, String> settings = new TreeMap<>();

//    id,first_name,last_name,email,gender,ip_address,last_login,account_balance,country,favorite_color
//    1,Jack,Garcia,jgarcia0@shop-pro.jp,Male,196.56.44.185,2015-09-30T15:29:03Z,$347.77,IT,#4a2313

    CSVFieldConfig.add(settings, index, CSVFieldConfig.NAME_CONF, "id");
    CSVFieldConfig.add(settings, index, CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.INT32.name());
    CSVFieldConfig.add(settings, index, CSVFieldConfig.REQUIRED_CONF, Boolean.TRUE.toString());
    index++;

    CSVFieldConfig.add(settings, index, CSVFieldConfig.NAME_CONF, "first_name");
    CSVFieldConfig.add(settings, index, CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.STRING.name());
    CSVFieldConfig.add(settings, index, CSVFieldConfig.REQUIRED_CONF, Boolean.TRUE.toString());
    index++;

    CSVFieldConfig.add(settings, index, CSVFieldConfig.NAME_CONF, "last_name");
    CSVFieldConfig.add(settings, index, CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.STRING.name());
    CSVFieldConfig.add(settings, index, CSVFieldConfig.REQUIRED_CONF, Boolean.TRUE.toString());
    index++;

    CSVFieldConfig.add(settings, index, CSVFieldConfig.NAME_CONF, "email");
    CSVFieldConfig.add(settings, index, CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.STRING.name());
    CSVFieldConfig.add(settings, index, CSVFieldConfig.REQUIRED_CONF, Boolean.TRUE.toString());
    index++;

    CSVFieldConfig.add(settings, index, CSVFieldConfig.NAME_CONF, "gender");
    CSVFieldConfig.add(settings, index, CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.STRING.name());
    CSVFieldConfig.add(settings, index, CSVFieldConfig.REQUIRED_CONF, Boolean.TRUE.toString());
    index++;

    CSVFieldConfig.add(settings, index, CSVFieldConfig.NAME_CONF, "ip_address");
    CSVFieldConfig.add(settings, index, CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.STRING.name());
    CSVFieldConfig.add(settings, index, CSVFieldConfig.REQUIRED_CONF, Boolean.TRUE.toString());
    index++;

    CSVFieldConfig.add(settings, index, CSVFieldConfig.NAME_CONF, "last_login");
    CSVFieldConfig.add(settings, index, CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.STRING.name());
    CSVFieldConfig.add(settings, index, CSVFieldConfig.LOGICAL_TYPE_CONF, CSVFieldConfig.LogicalType.timestamp.toString());
    CSVFieldConfig.add(settings, index, CSVFieldConfig.REQUIRED_CONF, Boolean.FALSE.toString());
    index++;

    CSVFieldConfig.add(settings, index, CSVFieldConfig.NAME_CONF, "account_balance");
    CSVFieldConfig.add(settings, index, CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.STRING.name());
    CSVFieldConfig.add(settings, index, CSVFieldConfig.LOGICAL_TYPE_CONF, CSVFieldConfig.LogicalType.decimal.toString());
    CSVFieldConfig.add(settings, index, CSVFieldConfig.REQUIRED_CONF, Boolean.FALSE.toString());
    index++;

    CSVFieldConfig.add(settings, index, CSVFieldConfig.NAME_CONF, "country");
    CSVFieldConfig.add(settings, index, CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.STRING.name());
    CSVFieldConfig.add(settings, index, CSVFieldConfig.REQUIRED_CONF, Boolean.TRUE.toString());
    index++;

    CSVFieldConfig.add(settings, index, CSVFieldConfig.NAME_CONF, "favorite_color");
    CSVFieldConfig.add(settings, index, CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.STRING.name());
    CSVFieldConfig.add(settings, index, CSVFieldConfig.REQUIRED_CONF, Boolean.FALSE.toString());
    index++;

    return settings;
  }

}
