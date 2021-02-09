package com.github.jcustenborder.kafka.connect.spooldir;

import shaded.com.google.common.collect.ImmutableList;
import shaded.com.google.common.io.Files;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FileComparatorTest {
  File tempDirectory;

  @BeforeEach
  public void before() {
    this.tempDirectory = Files.createTempDir();
  }

  File createFile(String name) throws IOException {
    return createFile(name, new Date().getTime(), 0);
  }

  File createFile(String name, long date) throws IOException {
    return createFile(name, date, 0);
  }

  File createFile(String name, long date, long length) throws IOException {
    File result = new File(tempDirectory, name);

    if (length == 0) {
      Files.touch(result);
    } else {
      Files.write(
          new byte[(int) length],
          result
      );
    }
    result.setLastModified(date);
    return result;
  }


  List<File> sort(List<File> files, AbstractSourceConnectorConfig.FileAttribute... attributes) {
    List<File> result = new ArrayList<>(files);
    FileComparator comparator = new FileComparator(ImmutableList.copyOf(attributes));
    result.sort(comparator);
    return result;
  }

  List<File> expected(List<File> files, int... indexes) {
    List<File> result = new ArrayList<>();
    for (int index : indexes) {
      result.add(files.get(index));
    }
    return result;
  }

  @Test
  public void existingFunctionality() throws IOException {
    List<File> input = Arrays.asList(
        createFile("File1.csv"),
        createFile("File2.csv"),
        createFile("File3.csv")
    );

    List<File> expected = expected(input, 0, 1, 2);
    List<File> actual = sort(input, AbstractSourceConnectorConfig.FileAttribute.NameAsc);
    assertEquals(expected, actual);
  }

  @Test
  public void sortByLastModified() throws IOException {
    long lastModified = new Date().getTime();

    List<File> input = Arrays.asList(
        createFile("File1.csv", lastModified-=1000L),
        createFile("File2.csv", lastModified-=1000L),
        createFile("File3.csv", lastModified-=1000L)
    );

    List<File> expected = expected(input, 2, 1, 0);
    List<File> actual = sort(input, AbstractSourceConnectorConfig.FileAttribute.LastModifiedAsc);
    assertEquals(expected, actual);
  }
  @Test
  public void sortBySize() throws IOException {
    long lastModified = new Date().getTime();
    long length = 10000;

    List<File> input = Arrays.asList(
        createFile("File1.csv", lastModified-=1000L, length-=1000L),
        createFile("File2.csv", lastModified-=1000L, length-=1000L),
        createFile("File3.csv", lastModified-=1000L, length-=1000L)
    );

    List<File> expected = expected(input, 0, 1, 2);
    List<File> actual = sort(input, AbstractSourceConnectorConfig.FileAttribute.LengthDesc);
    assertEquals(expected, actual);
  }


}
