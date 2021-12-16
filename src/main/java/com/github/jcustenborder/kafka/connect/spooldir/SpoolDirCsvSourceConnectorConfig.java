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

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.ValidEnum;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.ICSVParser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.DataException;

import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Map;

class SpoolDirCsvSourceConnectorConfig extends AbstractSpoolDirSourceConnectorConfig {

  //CSVRecordProcessorConfig
  public static final String CSV_SKIP_LINES_CONF = "csv.skip.lines";
  public static final String CSV_SEPARATOR_CHAR_CONF = "csv.separator.char";
  public static final String CSV_QUOTE_CHAR_CONF = "csv.quote.char";
  public static final String CSV_ESCAPE_CHAR_CONF = "csv.escape.char";
  public static final String CSV_STRICT_QUOTES_CONF = "csv.strict.quotes";
  public static final String CSV_IGNORE_LEADING_WHITESPACE_CONF = "csv.ignore.leading.whitespace";
  public static final String CSV_IGNORE_QUOTATIONS_CONF = "csv.ignore.quotations";
  public static final String CSV_KEEP_CARRIAGE_RETURN_CONF = "csv.keep.carriage.return";
  public static final String CSV_VERIFY_READER_CONF = "csv.verify.reader";
  public static final String CSV_NULL_FIELD_INDICATOR_CONF = "csv.null.field.indicator";
  public static final String CSV_FIRST_ROW_AS_HEADER_CONF = "csv.first.row.as.header";
  public static final String CSV_CHARSET_CONF = "csv.file.charset";
  public static final String CSV_CASE_SENSITIVE_FIELD_NAMES_CONF = "csv.case.sensitive.field.names";
  public static final String CSV_USE_RFC_4180_PARSER_CONF = "csv.rfc.4180.parser.enabled";
  static final String CSV_SKIP_LINES_DISPLAY = "Skip lins";
  static final String CSV_SEPARATOR_CHAR_DISPLAY = "Separator Character";
  static final String CSV_QUOTE_CHAR_DISPLAY = "Quote Character";
  static final String CSV_ESCAPE_CHAR_DISPLAY = "Escape Character";
  static final String CSV_STRICT_QUOTES_DISPLAY = "Strict Quotes";
  static final String CSV_IGNORE_LEADING_WHITESPACE_DISPLAY = "Ignore leading whitespace";
  static final String CSV_IGNORE_QUOTATIONS_DISPLAY = "Ignore quotations";
  static final String CSV_KEEP_CARRIAGE_RETURN_DISPLAY = "Preserve Carriage Return?";
  static final String CSV_VERIFY_READER_DISPLAY = "Verify reader";
  static final String CSV_NULL_FIELD_INDICATOR_DISPLAY = "Null field indicator";
  static final String CSV_FIRST_ROW_AS_HEADER_DISPLAY = "Treat first row as header.";
  static final String CSV_CHARSET_DISPLAY = "File character set.";
  static final String CSV_CASE_SENSITIVE_FIELD_NAMES_DISPLAY = "Case sensitive field names.";
  static final String CSV_USE_RFC_4180_PARSER_DISPLAY = "Flag to determine if the RFC 4180 should be " +
      "used instead.";
  static final Object CSV_USE_RFC_4180_PARSER_DEFAULT = false;


  static final String CSV_SKIP_LINES_DOC = "Number of lines to skip in the beginning of the file.";
  static final int CSV_SKIP_LINES_DEFAULT = CSVReader.DEFAULT_SKIP_LINES;
  static final String CSV_SEPARATOR_CHAR_DOC = "The character that separates each field in the form " +
      "of an integer. Typically in a CSV this is a ,(44) character. A TSV would use a tab(9) character. " +
      "If `" + CSV_SEPARATOR_CHAR_CONF + "` is defined as a null(0), then the RFC 4180 parser must be " +
      "utilized by default. This is the equivalent of `" + CSV_USE_RFC_4180_PARSER_CONF + " = true`.";
  static final int CSV_SEPARATOR_CHAR_DEFAULT = CSVParser.DEFAULT_SEPARATOR;
  static final int CSV_QUOTE_CHAR_DEFAULT = CSVParser.DEFAULT_QUOTE_CHARACTER;
  static final String CSV_ESCAPE_CHAR_DOC = "The character as an integer to use when a special " +
      "character is encountered. The default escape character is typically a \\(92)";
  static final int CSV_ESCAPE_CHAR_DEFAULT = CSVParser.DEFAULT_ESCAPE_CHARACTER;
  static final String CSV_STRICT_QUOTES_DOC = "Sets the strict quotes setting - if true, characters outside the quotes are ignored.";
  static final boolean CSV_STRICT_QUOTES_DEFAULT = CSVParser.DEFAULT_STRICT_QUOTES;
  static final String CSV_IGNORE_LEADING_WHITESPACE_DOC = "Sets the ignore leading whitespace setting - if true, white space in front of a quote in a field is ignored.";
  static final boolean CSV_IGNORE_LEADING_WHITESPACE_DEFAULT = CSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE;
  static final String CSV_IGNORE_QUOTATIONS_DOC = "Sets the ignore quotations mode - if true, quotations are ignored.";
  static final boolean CSV_IGNORE_QUOTATIONS_DEFAULT = CSVParser.DEFAULT_IGNORE_QUOTATIONS;
  static final String CSV_KEEP_CARRIAGE_RETURN_DOC = "Flag to determine if the carriage return at the end of the line should be maintained.";
  static final boolean CSV_KEEP_CARRIAGE_RETURN_DEFAULT = CSVReader.DEFAULT_KEEP_CR;
  static final String CSV_VERIFY_READER_DOC = "Flag to determine if the reader should be verified.";
  static final boolean CSV_VERIFY_READER_DEFAULT = CSVReader.DEFAULT_VERIFY_READER;
  static final String CSV_NULL_FIELD_INDICATOR_DOC = "Indicator to determine how the CSV Reader can determine if a field is null. Valid values are " + ConfigUtils.enumValues(CSVReaderNullFieldIndicator.class)
      + ". For more information see http://opencsv.sourceforge.net/apidocs/com/opencsv/enums/CSVReaderNullFieldIndicator.html.";
  static final String CSV_NULL_FIELD_INDICATOR_DEFAULT = CSVParser.DEFAULT_NULL_FIELD_INDICATOR.name();
  static final String CSV_FIRST_ROW_AS_HEADER_DOC = "Flag to indicate if the fist row of data contains the header of the file. " +
      "If true the position of the columns will be determined by the first row to the CSV. The column position will be inferred " +
      "from the position of the schema supplied in `" + VALUE_SCHEMA_CONF + "`. If set to true the number of columns must be " +
      "greater than or equal to the number of fields in the schema.";
  static final boolean CSV_FIRST_ROW_AS_HEADER_DEFAULT = false;
  static final String CSV_CHARSET_DOC = "Character set to read wth file with.";
  static final String CSV_CHARSET_DEFAULT = Charset.defaultCharset().name();

  static final String CSV_CASE_SENSITIVE_FIELD_NAMES_DOC = "Flag to determine if the field names in the header row should be treated as case sensitive.";
  static final String CSV_USE_RFC_4180_PARSER_DOC = "Flag to determine if the RFC 4180 parser should be used instead of the default parser.";
  static final String CSV_GROUP = "CSV Parsing";
  private static final String CSV_QUOTE_CHAR_DOC = "The character that is used to quote a field. This typically happens when the " + CSV_SEPARATOR_CHAR_CONF + " character is within the data.";
  private static final Character NULL_CHAR = (char) 0;
  public final int skipLines;
  public final char separatorChar;
  public final char quoteChar;
  public final char escapeChar;
  public final boolean ignoreLeadingWhitespace;
  public final boolean ignoreQuotations;
  public final boolean strictQuotes;
  public final boolean keepCarriageReturn;
  public final boolean verifyReader;
  public final CSVReaderNullFieldIndicator nullFieldIndicator;
  public final boolean firstRowAsHeader;
  public final Charset charset;
  public final boolean caseSensitiveFieldNames;
  public final boolean useRFC4180Parser;

  public SpoolDirCsvSourceConnectorConfig(final boolean isTask, Map<String, ?> settings) {
    super(isTask, true, config(), settings);
    this.skipLines = this.getInt(SpoolDirCsvSourceConnectorConfig.CSV_SKIP_LINES_CONF);
    this.separatorChar = this.getChar(SpoolDirCsvSourceConnectorConfig.CSV_SEPARATOR_CHAR_CONF);
    this.quoteChar = this.getChar(SpoolDirCsvSourceConnectorConfig.CSV_QUOTE_CHAR_CONF);
    this.escapeChar = this.getChar(SpoolDirCsvSourceConnectorConfig.CSV_ESCAPE_CHAR_CONF);
    this.ignoreLeadingWhitespace = this.getBoolean(SpoolDirCsvSourceConnectorConfig.CSV_IGNORE_LEADING_WHITESPACE_CONF);
    this.ignoreQuotations = this.getBoolean(SpoolDirCsvSourceConnectorConfig.CSV_IGNORE_QUOTATIONS_CONF);
    this.strictQuotes = this.getBoolean(SpoolDirCsvSourceConnectorConfig.CSV_STRICT_QUOTES_CONF);
    this.keepCarriageReturn = this.getBoolean(SpoolDirCsvSourceConnectorConfig.CSV_KEEP_CARRIAGE_RETURN_CONF);
    this.verifyReader = this.getBoolean(SpoolDirCsvSourceConnectorConfig.CSV_VERIFY_READER_CONF);
    this.nullFieldIndicator = ConfigUtils.getEnum(CSVReaderNullFieldIndicator.class, this, SpoolDirCsvSourceConnectorConfig.CSV_NULL_FIELD_INDICATOR_CONF);
    this.firstRowAsHeader = this.getBoolean(SpoolDirCsvSourceConnectorConfig.CSV_FIRST_ROW_AS_HEADER_CONF);

    this.charset = ConfigUtils.charset(this, SpoolDirCsvSourceConnectorConfig.CSV_CHARSET_CONF);

    this.caseSensitiveFieldNames = this.getBoolean(SpoolDirCsvSourceConnectorConfig.CSV_CASE_SENSITIVE_FIELD_NAMES_CONF);
    this.useRFC4180Parser = this.getBoolean(CSV_USE_RFC_4180_PARSER_CONF);
  }

  static ConfigDef config() {

    return AbstractSpoolDirSourceConnectorConfig.config(true)
        .define(
            ConfigKeyBuilder.of(CSV_SKIP_LINES_CONF, ConfigDef.Type.INT)
                .defaultValue(CSV_SKIP_LINES_DEFAULT)
                .importance(ConfigDef.Importance.LOW)
                .documentation(CSV_SKIP_LINES_DOC)
                .group(CSV_GROUP)
                .width(ConfigDef.Width.LONG)
                .displayName(CSV_SKIP_LINES_DISPLAY)
                .build()
        )
        .define(
            ConfigKeyBuilder.of(CSV_SEPARATOR_CHAR_CONF, ConfigDef.Type.INT)
                .defaultValue(CSV_SEPARATOR_CHAR_DEFAULT)
                .importance(ConfigDef.Importance.LOW)
                .documentation(CSV_SEPARATOR_CHAR_DOC)
                .group(CSV_GROUP)
                .width(ConfigDef.Width.LONG)
                .displayName(CSV_SEPARATOR_CHAR_DISPLAY)
                .build()
        )
        .define(
            ConfigKeyBuilder.of(CSV_QUOTE_CHAR_CONF, ConfigDef.Type.INT)
                .defaultValue(CSV_QUOTE_CHAR_DEFAULT)
                .importance(ConfigDef.Importance.LOW)
                .documentation(CSV_QUOTE_CHAR_DOC)
                .group(CSV_GROUP)
                .width(ConfigDef.Width.LONG)
                .displayName(CSV_QUOTE_CHAR_DISPLAY)
                .build()
        )
        .define(
            ConfigKeyBuilder.of(CSV_ESCAPE_CHAR_CONF, ConfigDef.Type.INT)
                .defaultValue(CSV_ESCAPE_CHAR_DEFAULT)
                .importance(ConfigDef.Importance.LOW)
                .documentation(CSV_ESCAPE_CHAR_DOC)
                .group(CSV_GROUP)
                .displayName(CSV_ESCAPE_CHAR_DISPLAY)
                .width(ConfigDef.Width.LONG)
                .build()
        )
        .define(
            ConfigKeyBuilder.of(CSV_STRICT_QUOTES_CONF, ConfigDef.Type.BOOLEAN)
                .defaultValue(CSV_STRICT_QUOTES_DEFAULT)
                .importance(ConfigDef.Importance.LOW)
                .documentation(CSV_STRICT_QUOTES_DOC)
                .displayName(CSV_STRICT_QUOTES_DISPLAY)
                .group(CSV_GROUP)
                .width(ConfigDef.Width.LONG)
                .build()
        )
        .define(
            ConfigKeyBuilder.of(CSV_IGNORE_LEADING_WHITESPACE_CONF, ConfigDef.Type.BOOLEAN)
                .defaultValue(CSV_IGNORE_LEADING_WHITESPACE_DEFAULT)
                .importance(ConfigDef.Importance.LOW)
                .documentation(CSV_IGNORE_LEADING_WHITESPACE_DOC)
                .group(CSV_GROUP)
                .displayName(CSV_IGNORE_LEADING_WHITESPACE_DISPLAY)
                .width(ConfigDef.Width.LONG)
                .build()
        )
        .define(
            ConfigKeyBuilder.of(CSV_IGNORE_QUOTATIONS_CONF, ConfigDef.Type.BOOLEAN)
                .defaultValue(CSV_IGNORE_QUOTATIONS_DEFAULT)
                .importance(ConfigDef.Importance.LOW)
                .documentation(CSV_IGNORE_QUOTATIONS_DOC)
                .displayName(CSV_IGNORE_QUOTATIONS_DISPLAY)
                .group(CSV_GROUP)
                .width(ConfigDef.Width.LONG)
                .build()
        )
        .define(
            ConfigKeyBuilder.of(CSV_KEEP_CARRIAGE_RETURN_CONF, ConfigDef.Type.BOOLEAN)
                .defaultValue(CSV_KEEP_CARRIAGE_RETURN_DEFAULT)
                .importance(ConfigDef.Importance.LOW)
                .documentation(CSV_KEEP_CARRIAGE_RETURN_DOC)
                .displayName(CSV_KEEP_CARRIAGE_RETURN_DISPLAY)
                .group(CSV_GROUP)
                .width(ConfigDef.Width.LONG)
                .build()
        )
        .define(
            ConfigKeyBuilder.of(CSV_VERIFY_READER_CONF, ConfigDef.Type.BOOLEAN)
                .defaultValue(CSV_VERIFY_READER_DEFAULT)
                .importance(ConfigDef.Importance.LOW)
                .documentation(CSV_VERIFY_READER_DOC)
                .group(CSV_GROUP)
                .displayName(CSV_VERIFY_READER_DISPLAY)
                .width(ConfigDef.Width.LONG)
                .build()
        )
        .define(
            ConfigKeyBuilder.of(CSV_NULL_FIELD_INDICATOR_CONF, ConfigDef.Type.STRING)
                .defaultValue(CSV_NULL_FIELD_INDICATOR_DEFAULT)
                .validator(ValidEnum.of(CSVReaderNullFieldIndicator.class))
                .importance(ConfigDef.Importance.LOW)
                .documentation(CSV_NULL_FIELD_INDICATOR_DOC)
                .displayName(CSV_NULL_FIELD_INDICATOR_DISPLAY)
                .group(CSV_GROUP)
                .width(ConfigDef.Width.LONG)
                .build()
        )
        .define(
            ConfigKeyBuilder.of(CSV_FIRST_ROW_AS_HEADER_CONF, ConfigDef.Type.BOOLEAN)
                .defaultValue(CSV_FIRST_ROW_AS_HEADER_DEFAULT)
                .importance(ConfigDef.Importance.MEDIUM)
                .documentation(CSV_FIRST_ROW_AS_HEADER_DOC)
                .displayName(CSV_FIRST_ROW_AS_HEADER_DISPLAY)
                .group(CSV_GROUP)
                .width(ConfigDef.Width.LONG)
                .build()
        )
        .define(
            ConfigKeyBuilder.of(CSV_CHARSET_CONF, ConfigDef.Type.STRING)
                .defaultValue(CSV_CHARSET_DEFAULT)
                .validator(CharsetValidator.of())
                .importance(ConfigDef.Importance.LOW)
                .documentation(CSV_CHARSET_DOC)
                .displayName(CSV_CHARSET_DISPLAY)
                .group(CSV_GROUP)
                .width(ConfigDef.Width.LONG)
                .build()
        )
        .define(
            ConfigKeyBuilder.of(CSV_CASE_SENSITIVE_FIELD_NAMES_CONF, ConfigDef.Type.BOOLEAN)
                .defaultValue(false)
                .importance(ConfigDef.Importance.LOW)
                .documentation(CSV_CASE_SENSITIVE_FIELD_NAMES_DOC)
                .displayName(CSV_CASE_SENSITIVE_FIELD_NAMES_DISPLAY)
                .build()
        )
        .define(
            ConfigKeyBuilder.of(CSV_USE_RFC_4180_PARSER_CONF, ConfigDef.Type.BOOLEAN)
                .defaultValue(CSV_USE_RFC_4180_PARSER_DEFAULT)
                .importance(ConfigDef.Importance.LOW)
                .documentation(CSV_USE_RFC_4180_PARSER_DOC)
                .displayName(CSV_USE_RFC_4180_PARSER_DISPLAY)
                .build()
        );
  }

  final char getChar(String key) {
    int intValue = this.getInt(key);
    return (char) intValue;
  }

  public ICSVParser createCSVParserBuilder() {
    final ICSVParser result;

    if (NULL_CHAR.equals(this.separatorChar) || this.useRFC4180Parser) {
      result = new RFC4180ParserBuilder()
          .withQuoteChar(this.quoteChar)
          .withSeparator(this.separatorChar)
          .withFieldAsNull(this.nullFieldIndicator)
          .build();
    } else {
      result = new CSVParserBuilder()
          .withEscapeChar(this.escapeChar)
          .withIgnoreLeadingWhiteSpace(this.ignoreLeadingWhitespace)
          .withIgnoreQuotations(this.ignoreQuotations)
          .withQuoteChar(this.quoteChar)
          .withSeparator(this.separatorChar)
          .withStrictQuotes(this.strictQuotes)
          .withFieldAsNull(this.nullFieldIndicator)
          .build();
    }

    return result;
  }

  public CSVReaderBuilder createCSVReaderBuilder(Reader reader, ICSVParser parser) {
    return new CSVReaderBuilder(reader)
        .withCSVParser(parser)
        .withKeepCarriageReturn(this.keepCarriageReturn)
        .withSkipLines(this.skipLines)
        .withVerifyReader(this.verifyReader)
        .withFieldAsNull(nullFieldIndicator);
  }

  @Override
  public boolean schemasRequired() {
    return true;
  }

  static class CharsetValidator implements ConfigDef.Validator {
    static CharsetValidator of() {
      return new CharsetValidator();
    }

    @Override
    public void ensureValid(String s, Object o) {
      try {
        Preconditions.checkState(o instanceof String);
        String input = (String) o;
        Charset.forName(input);
      } catch (IllegalArgumentException e) {
        throw new DataException(
            String.format("Charset '%s' is invalid for %s", o, s),
            e
        );
      }
    }

    @Override
    public String toString() {
      return Joiner.on(",").join(Charset.availableCharsets().keySet());
    }
  }
}
