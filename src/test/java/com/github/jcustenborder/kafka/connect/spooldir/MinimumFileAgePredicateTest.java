package com.github.jcustenborder.kafka.connect.spooldir;

import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MinimumFileAgePredicateTest {
  File inputFile;


  static final String EXTENSION = "processing";

  @BeforeEach
  public void before() throws IOException {
    this.inputFile = File.createTempFile("test", "file");
  }

  @AfterEach
  public void after() throws IOException {
    if (null != this.inputFile && this.inputFile.exists()) {
      this.inputFile.delete();
    }
  }

  Time time(long milliseconds) {
    Time time = mock(Time.class);
    when(time.milliseconds()).thenReturn(milliseconds);
    return time;
  }

  @Test
  public void notOldEnough() throws IOException {
    long timestamp = 1559653835123L;
    Time time = time(timestamp);
    this.inputFile.setLastModified(timestamp);
    InputFileDequeue.MinimumFileAgePredicate predicate = new InputFileDequeue.MinimumFileAgePredicate(
        1000,
        time
    );
    assertFalse(predicate.test(this.inputFile), "File should not be old enough");
  }

  @Test
  public void oldEnough() throws IOException {
    long timestamp = 1559653835123L;
    this.inputFile.setLastModified(timestamp);
    timestamp += 5000L;
    Time time = time(timestamp);
    InputFileDequeue.MinimumFileAgePredicate predicate = new InputFileDequeue.MinimumFileAgePredicate(
        1000,
        time
    );
    assertTrue(predicate.test(this.inputFile), "File should be old enough");
  }
}
