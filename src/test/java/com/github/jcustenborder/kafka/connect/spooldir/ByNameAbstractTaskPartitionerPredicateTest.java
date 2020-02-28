package com.github.jcustenborder.kafka.connect.spooldir;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class ByNameAbstractTaskPartitionerPredicateTest {
  protected List<File> input;


  @TestFactory
  public Stream<DynamicTest> test() {
    List<File> files = new ArrayList<>(500);
    for (int i = 0; i < 500; i++) {
      files.add(new File(UUID.randomUUID().toString()));
    }
    return IntStream.range(2, 50).boxed().map(count -> dynamicTest(count.toString(), () -> {
      Set<File> queue = new LinkedHashSet<>(files);
      for (int index = 0; index <= count; index++) {
        AbstractTaskPartitionerPredicate.ByName predicate = new AbstractTaskPartitionerPredicate.ByName(index, count);
        files.stream()
            .filter(predicate)
            .forEach(queue::remove);
      }
      assertEquals(0, queue.size(), "Queue should be empty");
    }));
  }


}
