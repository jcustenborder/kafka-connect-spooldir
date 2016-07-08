package io.confluent.kafka.connect.source;

import org.junit.Test;

public class DocumentationTests {

  @Test
  public void generateRST() {
    System.out.println(SpoolDirectoryConfig.getConf().toRst());
  }


}
