package io.confluent.kafka.connect.source;

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


    return settings;
  }

}
