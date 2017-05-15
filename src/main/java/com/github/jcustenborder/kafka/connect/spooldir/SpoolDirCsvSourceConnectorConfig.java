/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.spooldir;

import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.ValidEnum;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import org.apache.kafka.common.config.ConfigDef;

import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Map;

class SpoolDirCsvSourceConnectorConfig extends SpoolDirSourceConnectorConfig {

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
  static final String CSV_SKIP_LINES_DOC = "Number of lines to skip in the beginning of the file.";
  static final int CSV_SKIP_LINES_DEFAULT = CSVReader.DEFAULT_SKIP_LINES;
  static final String CSV_SEPARATOR_CHAR_DOC = "The character that seperates each field. Typically in a CSV this is a , character. A TSV would use \\t.";
  static final int CSV_SEPARATOR_CHAR_DEFAULT = (int) CSVParser.DEFAULT_SEPARATOR;
  static final int CSV_QUOTE_CHAR_DEFAULT = (int) CSVParser.DEFAULT_QUOTE_CHARACTER;
  static final String CSV_ESCAPE_CHAR_DOC = "Escape character.";
  static final int CSV_ESCAPE_CHAR_DEFAULT = (int) CSVParser.DEFAULT_ESCAPE_CHARACTER;
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
  static final String CSV_GROUP = "csv";
  static final String CSV_DISPLAY_NAME = "CSV Settings";
  private static final String CSV_QUOTE_CHAR_DOC = "The character that is used to quote a field. This typically happens when the " + CSV_SEPARATOR_CHAR_CONF + " character is within the data.";
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

//  public List<String> schemaFromHeaderKeys() {
//    return this.getList(SpoolDirCsvSourceConnectorConfig.CSV_SCHEMA_FROM_HEADER_KEYS_CONF);
//  }

  public final boolean caseSensitiveFieldNames;


  public SpoolDirCsvSourceConnectorConfig(Map<String, ?> settings) {
    super(conf(), settings);
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

    String charsetName = this.getString(SpoolDirCsvSourceConnectorConfig.CSV_CHARSET_CONF);
    this.charset = Charset.forName(charsetName);


    this.caseSensitiveFieldNames = this.getBoolean(SpoolDirCsvSourceConnectorConfig.CSV_CASE_SENSITIVE_FIELD_NAMES_CONF);


  }

  static final ConfigDef conf() {
    int csvPosition = 0;

    return SpoolDirSourceConnectorConfig.config()
        .define(CSV_SKIP_LINES_CONF, ConfigDef.Type.INT, CSV_SKIP_LINES_DEFAULT, ConfigDef.Importance.LOW, CSV_SKIP_LINES_DOC, CSV_GROUP, csvPosition++, ConfigDef.Width.LONG, CSV_DISPLAY_NAME)
        .define(CSV_SEPARATOR_CHAR_CONF, ConfigDef.Type.INT, CSV_SEPARATOR_CHAR_DEFAULT, ConfigDef.Importance.LOW, CSV_SEPARATOR_CHAR_DOC, CSV_GROUP, csvPosition++, ConfigDef.Width.LONG, CSV_DISPLAY_NAME)
        .define(CSV_QUOTE_CHAR_CONF, ConfigDef.Type.INT, CSV_QUOTE_CHAR_DEFAULT, ConfigDef.Importance.LOW, CSV_QUOTE_CHAR_DOC, CSV_GROUP, csvPosition++, ConfigDef.Width.LONG, CSV_DISPLAY_NAME)
        .define(CSV_ESCAPE_CHAR_CONF, ConfigDef.Type.INT, CSV_ESCAPE_CHAR_DEFAULT, ConfigDef.Importance.LOW, CSV_ESCAPE_CHAR_DOC, CSV_GROUP, csvPosition++, ConfigDef.Width.LONG, CSV_DISPLAY_NAME)
        .define(CSV_STRICT_QUOTES_CONF, ConfigDef.Type.BOOLEAN, CSV_STRICT_QUOTES_DEFAULT, ConfigDef.Importance.LOW, CSV_STRICT_QUOTES_DOC, CSV_GROUP, csvPosition++, ConfigDef.Width.LONG, CSV_DISPLAY_NAME)
        .define(CSV_IGNORE_LEADING_WHITESPACE_CONF, ConfigDef.Type.BOOLEAN, CSV_IGNORE_LEADING_WHITESPACE_DEFAULT, ConfigDef.Importance.LOW, CSV_IGNORE_LEADING_WHITESPACE_DOC, CSV_GROUP, csvPosition++, ConfigDef.Width.LONG, CSV_DISPLAY_NAME)
        .define(CSV_IGNORE_QUOTATIONS_CONF, ConfigDef.Type.BOOLEAN, CSV_IGNORE_QUOTATIONS_DEFAULT, ConfigDef.Importance.LOW, CSV_IGNORE_QUOTATIONS_DOC, CSV_GROUP, csvPosition++, ConfigDef.Width.LONG, CSV_DISPLAY_NAME)
        .define(CSV_KEEP_CARRIAGE_RETURN_CONF, ConfigDef.Type.BOOLEAN, CSV_KEEP_CARRIAGE_RETURN_DEFAULT, ConfigDef.Importance.LOW, CSV_KEEP_CARRIAGE_RETURN_DOC, CSV_GROUP, csvPosition++, ConfigDef.Width.LONG, CSV_DISPLAY_NAME)
        .define(CSV_VERIFY_READER_CONF, ConfigDef.Type.BOOLEAN, CSV_VERIFY_READER_DEFAULT, ConfigDef.Importance.LOW, CSV_VERIFY_READER_DOC, CSV_GROUP, csvPosition++, ConfigDef.Width.LONG, CSV_DISPLAY_NAME)
        .define(CSV_NULL_FIELD_INDICATOR_CONF, ConfigDef.Type.STRING, CSV_NULL_FIELD_INDICATOR_DEFAULT, ValidEnum.of(CSVReaderNullFieldIndicator.class), ConfigDef.Importance.LOW, CSV_NULL_FIELD_INDICATOR_DOC, CSV_GROUP, csvPosition++, ConfigDef.Width.LONG, CSV_DISPLAY_NAME)
        .define(CSV_FIRST_ROW_AS_HEADER_CONF, ConfigDef.Type.BOOLEAN, CSV_FIRST_ROW_AS_HEADER_DEFAULT, ConfigDef.Importance.MEDIUM, CSV_FIRST_ROW_AS_HEADER_DOC, CSV_GROUP, csvPosition++, ConfigDef.Width.LONG, CSV_DISPLAY_NAME)
        .define(CSV_CHARSET_CONF, ConfigDef.Type.STRING, CSV_CHARSET_DEFAULT, ConfigDef.Importance.LOW, CSV_CHARSET_DOC, CSV_GROUP, csvPosition++, ConfigDef.Width.LONG, CSV_DISPLAY_NAME)
        .define(CSV_CASE_SENSITIVE_FIELD_NAMES_CONF, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW, CSV_CASE_SENSITIVE_FIELD_NAMES_DOC);
  }

  final char getChar(String key) {
    int intValue = this.getInt(key);
    return (char) intValue;
  }

  public CSVParserBuilder createCSVParserBuilder() {

    return new CSVParserBuilder()
        .withEscapeChar(this.escapeChar)
        .withIgnoreLeadingWhiteSpace(this.ignoreLeadingWhitespace)
        .withIgnoreQuotations(this.ignoreQuotations)
        .withQuoteChar(this.quoteChar)
        .withSeparator(this.separatorChar)
        .withStrictQuotes(this.strictQuotes)
        .withFieldAsNull(nullFieldIndicator);
  }

  public CSVReaderBuilder createCSVReaderBuilder(Reader reader, CSVParser parser) {
    return new CSVReaderBuilder(reader)
        .withCSVParser(parser)
        .withKeepCarriageReturn(this.keepCarriageReturn)
        .withSkipLines(this.skipLines)
        .withVerifyReader(this.verifyReader)
        .withFieldAsNull(nullFieldIndicator);
  }
}
