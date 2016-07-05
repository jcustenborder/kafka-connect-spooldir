package io.confluent.kafka.connect.source;

import java.io.InputStream;

public class Data {

  public static InputStream getMockData() {
    return Data.class.getResourceAsStream("MOCK_DATA.csv");
  }

}
