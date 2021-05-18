/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.spooldir.elf;

import com.github.jcustenborder.parsers.elf.ElfParser;
import com.github.jcustenborder.parsers.elf.LogEntry;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SchemaConversionBuilderTest {

  @TestFactory
  public Stream<DynamicTest> normalizeFieldName() {
    Map<String, String> tests = new LinkedHashMap<>();
    tests.put("date", "date");
    tests.put("time", "time");
    tests.put("x-edge-location", "x_edge_location");
    tests.put("sc-bytes", "sc_bytes");
    tests.put("c-ip", "c_ip");
    tests.put("cs-method", "cs_method");
    tests.put("cs(Host)", "cs_host");
    tests.put("cs-uri-stem", "cs_uri_stem");
    tests.put("sc-status", "sc_status");
    tests.put("cs(Referer)", "cs_referer");
    tests.put("cs(User-Agent)", "cs_user_agent");
    tests.put("cs-uri-query", "cs_uri_query");
    tests.put("cs(Cookie)", "cs_cookie");
    tests.put("x-edge-result-type", "x_edge_result_type");
    tests.put("x-edge-request-id", "x_edge_request_id");
    tests.put("x-host-header", "x_host_header");
    tests.put("cs-protocol", "cs_protocol");
    tests.put("cs-bytes", "cs_bytes");
    tests.put("time-taken", "time_taken");

    return tests.entrySet().stream().map(e -> dynamicTest(e.getKey(), () -> {
      final String actual = SchemaConversionBuilder.normalizeFieldName(e.getKey());
      assertEquals(e.getValue(), actual, "field name does not match.");
    }));
  }


  @Test
  public void foo() {
    ElfParser parser = mock(ElfParser.class);
    final Map<String, Class<?>> fieldTypes = ImmutableMap.of(
        "date", LocalDate.class,
        "time", LocalTime.class,
        "sc-bytes", Long.class,
        "sc-status", Integer.class
    );
    final Map<String, Object> fieldData = ImmutableMap.of(
        "date", LocalDate.of(2011, 3, 14),
        "time", LocalTime.of(12, 0, 0),
        "sc-bytes", 12341L,
        "sc-status", 200
    );
    when(parser.fieldTypes()).thenReturn(fieldTypes);

    SchemaConversionBuilder schemaGenerator = new SchemaConversionBuilder(parser);
    SchemaConversion conversion = schemaGenerator.build();
    assertNotNull(conversion, "conversion should not be null.");

    LogEntry entry = mock(LogEntry.class);
    when(entry.fieldTypes()).thenReturn(fieldTypes);
    when(entry.fieldData()).thenReturn(fieldData);

    SchemaAndValue actual = conversion.convert(entry);
    assertNotNull(actual, "actual should not be null");
//    assertNotNull(actual.getKey(), "actual.getKey() should not be null");
    assertNotNull(actual.schema(), "actual.getValue() should not be null");
    assertNotNull(actual.value(), "actual.getValue() should not be null");

//    actual.getValue()..validate();

//date time x-edge-location sc-bytes c-ip cs-method cs(Host) cs-uri-stem sc-status cs(Referer) cs(User-Agent) cs-uri-query cs(Cookie) x-edge-result-type x-edge-request-id x-host-header cs-protocol cs-bytes time-taken


  }


}
