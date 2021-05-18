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

import com.google.common.collect.ComparisonChain;

import java.io.File;
import java.util.Comparator;
import java.util.List;

class FileComparator implements Comparator<File> {
  final List<AbstractSourceConnectorConfig.FileAttribute> attributes;

  FileComparator(List<AbstractSourceConnectorConfig.FileAttribute> attributes) {
    this.attributes = attributes;
  }

  @Override
  public int compare(File f1, File f2) {
    ComparisonChain chain = ComparisonChain.start();

    for (AbstractSourceConnectorConfig.FileAttribute fileAttribute : this.attributes) {
      switch (fileAttribute) {
        case NameAsc:
          chain = chain.compare(f1.getName(), f2.getName());
          break;
        case NameDesc:
          chain = chain.compare(f2.getName(), f1.getName());
          break;
        case LengthAsc: // We prefer larger files first.
          chain = chain.compare(f1.length(), f2.length());
          break;
        case LengthDesc: // We prefer larger files first.
          chain = chain.compare(f2.length(), f1.length());
          break;
        case LastModifiedAsc:
          chain = chain.compare(f1.lastModified(), f2.lastModified());
          break;
        case LastModifiedDesc:
          chain = chain.compare(f2.lastModified(), f1.lastModified());
          break;
        default:
          throw new UnsupportedOperationException(
              String.format("%s is not a supported FileAttribute.", fileAttribute)
          );
      }
    }
    return chain.result();
  }
}
