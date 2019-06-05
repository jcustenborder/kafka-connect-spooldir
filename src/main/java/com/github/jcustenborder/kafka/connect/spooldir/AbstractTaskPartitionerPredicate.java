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
package com.github.jcustenborder.kafka.connect.spooldir;

import com.google.common.hash.Hashing;
import org.apache.kafka.common.config.ConfigException;

import java.io.File;
import java.util.function.Predicate;

abstract class AbstractTaskPartitionerPredicate implements Predicate<File> {
  final int index;
  final int count;

  protected AbstractTaskPartitionerPredicate(int index, int count) {
    this.index = index;
    this.count = count;
  }

  public static Predicate<File> create(AbstractSourceConnectorConfig config) {
    Predicate<File> result;

    if (config.taskCount == 1) {
      result = new None(config.taskIndex, config.taskCount);
    } else {
      switch (config.taskPartitioner) {
        case ByName:
          result = new ByName(config.taskIndex, config.taskCount);
          break;
        default:
          throw new ConfigException(
              AbstractSourceConnectorConfig.TASK_PARTITIONER_CONF,
              config.taskPartitioner.toString(),
              "Unsupported value"
          );
      }
    }

    return result;
  }

  /**
   * This implementation is used to bypass the check.
   */
  static class None extends AbstractTaskPartitionerPredicate {
    None(int index, int count) {
      super(index, count);
    }

    @Override
    public boolean test(File file) {
      return true;
    }
  }

  /**
   *
   */
  static class ByName extends AbstractTaskPartitionerPredicate {

    protected ByName(int index, int count) {
      super(index, count);
    }

    @Override
    public boolean test(File file) {
      final int hashcode = Math.abs(
          Hashing.adler32()
              .hashUnencodedChars(file.getName())
              .asInt()
      );
      return (hashcode % this.count) == index;
    }
  }


}
