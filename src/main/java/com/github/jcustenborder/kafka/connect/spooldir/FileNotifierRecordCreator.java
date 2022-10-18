/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.spooldir;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import static java.lang.System.currentTimeMillis;

public abstract class FileNotifierRecordCreator {
  public static final Logger log = LoggerFactory.getLogger(AbstractSourceTask.class);

  public abstract SourceRecord startFilePayload(InputFile file);

  public abstract SourceRecord endFilePayload(InputFile file);

  protected String topic;

  public void setTopic(String topic) {
    this.topic = topic;
  }

  /**
   * Simple implementation to notify basic details in JSON.
   */
  public static class DefaultFileNotifierRecordCreator extends FileNotifierRecordCreator {
    @Override
    public SourceRecord startFilePayload(InputFile file) {
      return new SourceRecord(
              new HashMap<>(),
              new HashMap<>(),
              this.topic,
              Schema.BYTES_SCHEMA,
              String.format(
                      "{\"time\": %d, \"event\": \"file_start\", \"filepath\": \"%s\"}",
                      currentTimeMillis(),
                      file.getPath()
              ).getBytes()
      );

    }

    @Override
    public SourceRecord endFilePayload(InputFile file) {
      return new SourceRecord(
              new HashMap<>(),
              new HashMap<>(),
              this.topic,
              Schema.BYTES_SCHEMA,
              String.format(
                      "{\"time\":%d, \"event\": \"file_end\", \"filepath\": \"%s\"}",
                      currentTimeMillis(),
                      file.getPath()
              ).getBytes()
      );
    }
  }

  public static FileNotifierRecordCreator newInstanceForClassName(String className) {
    try {
      Class<?> clazz = Class.forName(className);
      Object o = Utils.newInstance(clazz);
      if (!(o instanceof FileNotifierRecordCreator)) {
        String error = className + " is not an implementation of FileNotifierRecordCreator";
        log.error(error);
        throw new KafkaException(error);
      }
      return (FileNotifierRecordCreator) o;

    } catch (ClassNotFoundException e) {
      log.error("Unable to find class {} as a FileNotifierRecordCreator", className);
      throw new KafkaException(e);
    }

  }
}
