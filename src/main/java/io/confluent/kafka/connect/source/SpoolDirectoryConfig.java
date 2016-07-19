/**
 * Copyright (C) 2016 Jeremy Custenborder (jcustenborder@gmail.com)
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
package io.confluent.kafka.connect.source;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Preconditions;
import com.google.common.io.PatternFilenameFilter;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import io.confluent.kafka.connect.source.io.processing.RecordProcessor;
import io.confluent.kafka.connect.source.io.processing.csv.SchemaConfig;
import io.confluent.kafka.connect.utils.config.ConfigUtils;
import io.confluent.kafka.connect.utils.config.ValidEnum;
import io.confluent.kafka.connect.utils.config.ValidPattern;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.errors.ConnectException;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Pattern;


@SuppressWarnings("WeakerAccess")
public class SpoolDirectoryConfig extends AbstractConfig {

  //DirectoryMonitorConfig
  public static final String RECORD_PROCESSOR_CLASS_CONF = "record.processor.class";
  //PollingDirectoryMonitorConfig
  public static final String INPUT_PATH_CONFIG = "input.path";
  public static final String FINISHED_PATH_CONFIG = "finished.path";
  public static final String ERROR_PATH_CONFIG = "error.path";
  public static final String INPUT_FILE_PATTERN_CONF = "input.file.pattern";
  public static final String HALT_ON_ERROR_CONF = "halt.on.error";
  public static final String FILE_MINIMUM_AGE_MS_CONF = "file.minimum.age.ms";
  public static final String PROCESSING_FILE_EXTENSION_CONF = "processing.file.extension";
  //RecordProcessorConfig
  public static final String BATCH_SIZE_CONF = "batch.size";
  public static final String BATCH_SIZE_DOC = "The number of records that should be returned with each batch.";
  public static final int BATCH_SIZE_DEFAULT = 1000;
  public static final String PROCESSING_FILE_EXTENSION_DEFAULT = ".PROCESSING";
  public static final String TOPIC_CONF = "topic";
  public static final String TOPIC_DOC = "The Kafka topic to write the data to.";

  public static final String INCLUDE_FILE_METADATA_CONF = "include.file.metadata";
  public static final String INCLUDE_FILE_METADATA_DOC = "Flag to determine if the metadata about the file should be included.";
  public static final boolean INCLUDE_FILE_METADATA_DEFAULT = false;


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
  public static final String CSV_PARSER_TIMESTAMP_DATE_FORMATS_CONF = "csv.parser.timestamp.date.formats";
  public static final String CSV_PARSER_TIMESTAMP_TIMEZONE_CONF = "csv.parser.timestamp.timezone";
  public static final String CSV_SCHEMA_CONF = "csv.schema";
  public static final String CSV_SCHEMA_NAME_CONF = "csv.schema.name";
  public static final String CSV_SCHEMA_FROM_HEADER_CONF = "csv.schema.from.header";
  public static final String CSV_SCHEMA_FROM_HEADER_KEYS_CONF = "csv.schema.from.header.keys";
  public static final String CSV_CASE_SENSITIVE_FIELD_NAMES_CONF = "csv.case.sensitive.field.names";
  static final String RECORD_PROCESSOR_CLASS_DOC = "Class that implements RecordProcessor. This class is used to process data as it arrives.";
  static final String INPUT_PATH_DOC = "The directory to read files that will be processed. This directory must exist and be writable by the user running Kafka Connect.";
  static final String FINISHED_PATH_DOC = "The directory to place files that have been successfully processed. This directory must exist and be writable by the user running Kafka Connect.";
  static final String ERROR_PATH_DOC = "The directory to place files in which have error(s). This directory must exist and be writable by the user running Kafka Connect.";
  static final String INPUT_FILE_PATTERN_DOC = "Regular expression to check input file names against. This expression " +
      "must match the entire filename. The equivalent of Matcher.matches().";
  static final String HALT_ON_ERROR_DOC = "Should the task halt when it encounters an error or continue to the next file.";
  static final String FILE_MINIMUM_AGE_MS_DOC = "The amount of time in milliseconds after the file was last written to before the file can be processed.";
  static final String PROCESSING_FILE_EXTENSION_DOC = "Before a file is processed, it is renamed to indicate that it is currently being processed. This setting is appended to the end of the file.";
  static final String CSV_SKIP_LINES_DOC = "Number of lines to skip in the beginning of the file.";
  static final int CSV_SKIP_LINES_DEFAULT = CSVReader.DEFAULT_SKIP_LINES;
  static final String CSV_SEPARATOR_CHAR_DOC = "The character that seperates each field. Typically in a CSV this is a , character. A TSV would use \\t.";
  static final int CSV_SEPARATOR_CHAR_DEFAULT = (int) CSVParser.DEFAULT_SEPARATOR;
  static final String CSV_QUOTE_CHAR_DOC = "The character that is used to quote a field. This typically happens when the " + CSV_SEPARATOR_CHAR_CONF + " character is within the data.";
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
  static final String CSV_FIRST_ROW_AS_HEADER_DOC = "Flag to indicate if the fist row of data contains the header of the file.";
  static final boolean CSV_FIRST_ROW_AS_HEADER_DEFAULT = false;
  static final String CSV_CHARSET_DOC = "Character set to read wth file with.";
  static final String CSV_CHARSET_DEFAULT = Charset.defaultCharset().name();
  static final String CSV_PARSER_TIMESTAMP_DATE_FORMATS_DOC = "The date formats that are expected in the file. This is a list " +
      "of strings that will be used to parse the date fields in order. The most accurate date format should be the first " +
      "in the list. Take a look at the Java documentation for more info. " +
      "https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html";
  static final List<String> CSV_PARSER_TIMESTAMP_DATE_FORMATS_DEFAULT = Arrays.asList("yyyy-MM-dd' 'HH:mm:ss");
  static final String CSV_PARSER_TIMESTAMP_TIMEZONE_DOC = "The timezone that all of the dates will be parsed with.";
  static final String CSV_PARSER_TIMESTAMP_TIMEZONE_DEFAULT = "UTC";
  static final String CSV_SCHEMA_SCHEMA_NAME_DOC = "Fully qualified name for the schema. This setting is ignored if " + CSV_SCHEMA_CONF + " is set.";
  static final String CSV_SCHEMA_DOC = "Schema representation in json.";
  static final String CSV_INFER_SCHEMA_FROM_HEADER_DOC = "Flag to determine if the schema should be generated based on the header row.";
  static final String CSV_CASE_SENSITIVE_FIELD_NAMES_DOC = "Flag to determine if the field names in the header row should be treated as case sensitive.";
  static final String CSV_SCHEMA_FROM_HEADER_KEYS_DOC = "csv.schema.from.header.keys";
  static final String CSV_GROUP = "csv";
  static final String CSV_DISPLAY_NAME = "CSV Settings";

  public SpoolDirectoryConfig(Map<?, ?> parsedConfig) {
    super(getConf(), parsedConfig);
  }

  public static ConfigDef getConf() {

    int csvPosition = 0;

    return new ConfigDef()

        //DirectoryMonitorConfig
        .define(RECORD_PROCESSOR_CLASS_CONF, ConfigDef.Type.CLASS, ConfigDef.Importance.HIGH, RECORD_PROCESSOR_CLASS_DOC)

        //PollingDirectoryMonitorConfig
        .define(INPUT_PATH_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, INPUT_PATH_DOC)
        .define(FINISHED_PATH_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, FINISHED_PATH_DOC)
        .define(ERROR_PATH_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, ERROR_PATH_DOC)
        .define(INPUT_FILE_PATTERN_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, INPUT_FILE_PATTERN_DOC)
        .define(HALT_ON_ERROR_CONF, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH, HALT_ON_ERROR_DOC)
        .define(FILE_MINIMUM_AGE_MS_CONF, ConfigDef.Type.LONG, 0L, ConfigDef.Range.between(0L, Long.MAX_VALUE), ConfigDef.Importance.LOW, FILE_MINIMUM_AGE_MS_DOC)
        .define(PROCESSING_FILE_EXTENSION_CONF, ConfigDef.Type.STRING, PROCESSING_FILE_EXTENSION_DEFAULT, ValidPattern.of("^.*\\..+$"), ConfigDef.Importance.LOW, PROCESSING_FILE_EXTENSION_DOC)

        //RecordProcessorConfig
        .define(BATCH_SIZE_CONF, ConfigDef.Type.INT, BATCH_SIZE_DEFAULT, ConfigDef.Importance.LOW, BATCH_SIZE_DOC)
        .define(TOPIC_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TOPIC_DOC)
        .define(INCLUDE_FILE_METADATA_CONF, Type.BOOLEAN, INCLUDE_FILE_METADATA_DEFAULT, ConfigDef.Importance.LOW, INCLUDE_FILE_METADATA_DOC)

        //CSVRecordProcessorConfig
//    String group, int orderInGroup, ConfigDef.Width width, String displayName

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
        .define(CSV_PARSER_TIMESTAMP_TIMEZONE_CONF, ConfigDef.Type.STRING, CSV_PARSER_TIMESTAMP_TIMEZONE_DEFAULT, ConfigDef.Importance.LOW, CSV_PARSER_TIMESTAMP_TIMEZONE_DOC, CSV_GROUP, csvPosition++, ConfigDef.Width.LONG, CSV_DISPLAY_NAME)
        .define(CSV_PARSER_TIMESTAMP_DATE_FORMATS_CONF, ConfigDef.Type.LIST, CSV_PARSER_TIMESTAMP_DATE_FORMATS_DEFAULT, ConfigDef.Importance.LOW, CSV_PARSER_TIMESTAMP_DATE_FORMATS_DOC, CSV_GROUP, csvPosition++, ConfigDef.Width.LONG, CSV_DISPLAY_NAME)
        .define(CSV_SCHEMA_CONF, Type.STRING, "", ConfigDef.Importance.MEDIUM, CSV_SCHEMA_DOC)
        .define(CSV_SCHEMA_FROM_HEADER_CONF, Type.BOOLEAN, false, ConfigDef.Importance.LOW, CSV_INFER_SCHEMA_FROM_HEADER_DOC)
        .define(CSV_CASE_SENSITIVE_FIELD_NAMES_CONF, Type.BOOLEAN, false, ConfigDef.Importance.LOW, CSV_CASE_SENSITIVE_FIELD_NAMES_DOC)
        .define(CSV_SCHEMA_FROM_HEADER_KEYS_CONF, Type.LIST, new ArrayList<>(), ConfigDef.Importance.LOW, CSV_SCHEMA_FROM_HEADER_KEYS_DOC)
        .define(CSV_SCHEMA_NAME_CONF, Type.STRING, "", ConfigDef.Importance.LOW, CSV_SCHEMA_SCHEMA_NAME_DOC);
  }


  public File inputPath() {
    return ConfigUtils.getAbsoluteFile(this, INPUT_PATH_CONFIG);
  }

  public File finishedPath() {
    return ConfigUtils.getAbsoluteFile(this, FINISHED_PATH_CONFIG);
  }

  public File errorPath() {
    return ConfigUtils.getAbsoluteFile(this, ERROR_PATH_CONFIG);
  }

  public PatternFilenameFilter inputFilePattern() {
    String input = this.getString(INPUT_FILE_PATTERN_CONF);
    Pattern pattern = Pattern.compile(input);
    return new PatternFilenameFilter(pattern);
  }

  public boolean haltOnError() {
    return this.getBoolean(HALT_ON_ERROR_CONF);
  }

  public long minimumFileAgeMS() {
    return this.getLong(FILE_MINIMUM_AGE_MS_CONF);
  }

  public int batchSize() {
    return this.getInt(BATCH_SIZE_CONF);
  }

  public String topic() {
    return this.getString(TOPIC_CONF);
  }

  public int skipLines() {
    return this.getInt(CSV_SKIP_LINES_CONF);
  }

  char getChar(String key) {
    int intValue = this.getInt(key);
    return (char) intValue;
  }

  public char separatorChar() {
    return this.getChar(CSV_SEPARATOR_CHAR_CONF);
  }

  public char quoteChar() {
    return this.getChar(CSV_QUOTE_CHAR_CONF);
  }

  public char escapeChar() {
    return this.getChar(CSV_ESCAPE_CHAR_CONF);
  }

  public boolean ignoreLeadingWhitespace() {
    return this.getBoolean(CSV_IGNORE_LEADING_WHITESPACE_CONF);
  }

  public boolean ignoreQuotations() {
    return this.getBoolean(CSV_IGNORE_QUOTATIONS_CONF);
  }

  public boolean strictQuotes() {
    return this.getBoolean(CSV_STRICT_QUOTES_CONF);
  }

  public boolean keepCarriageReturn() {
    return this.getBoolean(CSV_KEEP_CARRIAGE_RETURN_CONF);
  }

  public boolean verifyReader() {
    return this.getBoolean(CSV_VERIFY_READER_CONF);
  }

  public CSVReaderNullFieldIndicator nullFieldIndicator() {
    return ConfigUtils.getEnum(CSVReaderNullFieldIndicator.class, this, CSV_NULL_FIELD_INDICATOR_CONF);
  }

  public boolean firstRowAsHeader() {
    return this.getBoolean(CSV_FIRST_ROW_AS_HEADER_CONF);
  }

  public List<String> schemaFromHeaderKeys() {
    return this.getList(CSV_SCHEMA_FROM_HEADER_KEYS_CONF);
  }

  public Charset charset() {
    String value = this.getString(CSV_CHARSET_CONF);
    return Charset.forName(value);
  }

  public CSVParserBuilder createCSVParserBuilder() {

    return new CSVParserBuilder()
        .withEscapeChar(this.escapeChar())
        .withIgnoreLeadingWhiteSpace(this.ignoreLeadingWhitespace())
        .withIgnoreQuotations(this.ignoreQuotations())
        .withQuoteChar(this.quoteChar())
        .withSeparator(this.separatorChar())
        .withStrictQuotes(this.strictQuotes())
        .withFieldAsNull(nullFieldIndicator());
  }

  public CSVReaderBuilder createCSVReaderBuilder(Reader reader, CSVParser parser) {
    return new CSVReaderBuilder(reader)
        .withCSVParser(parser)
        .withKeepCarriageReturn(this.keepCarriageReturn())
        .withSkipLines(this.skipLines())
        .withVerifyReader(this.verifyReader())
        .withFieldAsNull(nullFieldIndicator());
  }

  public SimpleDateFormat[] parserTimestampDateFormats() {
    List<SimpleDateFormat> results = new ArrayList<>();
    List<String> formats = this.getList(CSV_PARSER_TIMESTAMP_DATE_FORMATS_CONF);

    for (String s : formats) {
      results.add(new SimpleDateFormat(s));
    }

    return results.toArray(new SimpleDateFormat[results.size()]);
  }

  public TimeZone parserTimestampTimezone() {
    String value = this.getString(CSV_PARSER_TIMESTAMP_TIMEZONE_CONF);
    return TimeZone.getTimeZone(value);
  }

  public Class<RecordProcessor> recordProcessor() {
    return ConfigUtils.getClass(this, RECORD_PROCESSOR_CLASS_CONF, RecordProcessor.class);
  }

  public SchemaConfig schemaConfig() {
    String fieldContent = this.getString(CSV_SCHEMA_CONF);

    Preconditions.checkState(null != fieldContent && !fieldContent.isEmpty(), "%s must be configured with a valid schema.", CSV_SCHEMA_CONF);

    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.enable(SerializationFeature.WRITE_ENUMS_USING_TO_STRING);
    objectMapper.enable(DeserializationFeature.READ_ENUMS_USING_TO_STRING);
    try {
      return objectMapper.readValue(fieldContent, SchemaConfig.class);
    } catch (IOException e) {
      throw new ConnectException("Could not parse schemaConfig json", e);
    }
  }

  public boolean schemaFromHeader() {
    return this.getBoolean(CSV_SCHEMA_FROM_HEADER_CONF);
  }

  public boolean caseSensitiveFieldNames() {
    return this.getBoolean(CSV_CASE_SENSITIVE_FIELD_NAMES_CONF);
  }

  public String processingFileExtension() {
    return this.getString(PROCESSING_FILE_EXTENSION_CONF);
  }

  public boolean includeFileMetadata() {
    return this.getBoolean(INCLUDE_FILE_METADATA_CONF);
  }

  public String schemaName() {
    return this.getString(CSV_SCHEMA_NAME_CONF);
  }
}
