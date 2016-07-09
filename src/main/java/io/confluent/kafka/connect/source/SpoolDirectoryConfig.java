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
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.io.PatternFilenameFilter;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import io.confluent.kafka.connect.source.io.processing.RecordProcessor;
import io.confluent.kafka.connect.source.io.processing.csv.SchemaConfig;
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

  static String[] nullFieldIndicatorValues() {
    String[] result = new String[CSVReaderNullFieldIndicator.values().length];
    for (int i = 0; i < CSVReaderNullFieldIndicator.values().length; i++) {
      result[i] = CSVReaderNullFieldIndicator.values()[i].name();
    }
    return result;
  }

  //DirectoryMonitorConfig
  public static final String RECORD_PROCESSOR_CLASS_CONF = "record.processor.class";
  static final String RECORD_PROCESSOR_CLASS_DOC = "Class that implements RecordProcessor. This class is used to process data as it arrives.";

  //PollingDirectoryMonitorConfig
  public static final String INPUT_PATH_CONFIG = "input.path";
  public static final String FINISHED_PATH_CONFIG = "finished.path";
  public static final String ERROR_PATH_CONFIG = "error.path";
  public static final String INPUT_FILE_PATTERN_CONF = "input.file.pattern";
  public static final String HALT_ON_ERROR_CONF = "halt.on.error";
  public static final String FILE_MINIMUM_AGE_MS_CONF = "file.minimum.age.ms";
  static final String INPUT_PATH_DOC = "The directory to read files that will be processed. This directory must exist and be writable by the user running Kafka Connect.";
  static final String FINISHED_PATH_DOC = "The directory to place files that have been successfully processed. This directory must exist and be writable by the user running Kafka Connect.";
  static final String ERROR_PATH_DOC = "The directory to place files in which have error(s). This directory must exist and be writable by the user running Kafka Connect.";
  static final String INPUT_FILE_PATTERN_DOC = "Regular expression to check input file names against.";
  static final String HALT_ON_ERROR_DOC = "Should the task halt when it encounters an error or continue to the next file.";
  static final String FILE_MINIMUM_AGE_MS_DOC = "The amount of time in milliseconds after the file was last written to before the file can be processed.";

  //RecordProcessorConfig
  public static final String BATCH_SIZE_CONF = "batch.size";
  public static final String BATCH_SIZE_DOC = "The number of records that should be returned with each batch.";
  public static final int BATCH_SIZE_DEFAULT = 100;

  public static final String TOPIC_CONF = "topic";
  public static final String TOPIC_DOC = "The Kafka topic to write the data to.";

  //CSVRecordProcessorConfig
  public static final String SKIP_LINES_CONF = "skip.lines";
  public static final String SEPARATOR_CHAR_CONF = "separator.char";
  public static final String QUOTE_CHAR_CONF = "quote.char";
  public static final String ESCAPE_CHAR_CONF = "escape.char";
  public static final String STRICT_QUOTES_CONF = "strict.quotes";
  public static final String IGNORE_LEADING_WHITESPACE_CONF = "ignore.leading.whitespace";
  public static final String IGNORE_QUOTATIONS_CONF = "ignore.quotations";
  public static final String KEEP_CARRIAGE_RETURN_CONF = "keep.carriage.return";
  public static final String VERIFY_READER_CONF = "verify.reader";
  public static final String NULL_FIELD_INDICATOR_CONF = "null.field.indicator";
  public static final String KEY_FIELDS_CONF = "key.fields";
  public static final String FIRST_ROW_AS_HEADER_CONF = "first.row.as.header";
  public static final String CHARSET_CONF = "file.charset";
  public static final String PARSER_TIMESTAMP_DATE_FORMATS_CONF = "parser.timestamp.date.formats";
  public static final String PARSER_TIMESTAMP_TIMEZONE_CONF = "parser.timestamp.timezone";
  public static final String SCHEMA_CONF = "schema";
  public static final String INFER_SCHEMA_FROM_HEADER_CONF = "infer.structSchema.from.header";
  public static final String CASE_SENSITIVE_FIELD_NAMES_CONF = "case.sensitive.field.names";

  static final String SKIP_LINES_DOC = "Number of lines to skip in the beginning of the file.";
  static final int SKIP_LINES_DEFAULT = CSVReader.DEFAULT_SKIP_LINES;
  static final String SEPARATOR_CHAR_DOC = "The character that seperates each field. Typically in a CSV this is a , character. A TSV would use \\t.";
  static final int SEPARATOR_CHAR_DEFAULT = (int) CSVParser.DEFAULT_SEPARATOR;
  static final String QUOTE_CHAR_DOC = "The character that is used to quote a field. This typically happens when the " + SEPARATOR_CHAR_CONF + " character is within the data.";
  static final int QUOTE_CHAR_DEFAULT = (int) CSVParser.DEFAULT_QUOTE_CHARACTER;
  static final String ESCAPE_CHAR_DOC = "Escape character.";
  static final int ESCAPE_CHAR_DEFAULT = (int) CSVParser.DEFAULT_ESCAPE_CHARACTER;
  static final String STRICT_QUOTES_DOC = "strict quotes.";
  static final boolean STRICT_QUOTES_DEFAULT = CSVParser.DEFAULT_STRICT_QUOTES;
  static final String IGNORE_LEADING_WHITESPACE_DOC = "Flag to determine if the whitespace leading the field should be ignored.";
  static final boolean IGNORE_LEADING_WHITESPACE_DEFAULT = CSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE;
  static final String IGNORE_QUOTATIONS_DOC = "ignore_quotations character.";
  static final boolean IGNORE_QUOTATIONS_DEFAULT = CSVParser.DEFAULT_IGNORE_QUOTATIONS;
  static final String KEEP_CARRIAGE_RETURN_DOC = "Flag to determine if the carriage return at the end of the line should be maintained.";
  static final boolean KEEP_CARRIAGE_RETURN_DEFAULT = CSVReader.DEFAULT_KEEP_CR;
  static final String VERIFY_READER_DOC = "Flag to determine if the reader should be verified.";
  static final boolean VERIFY_READER_DEFAULT = CSVReader.DEFAULT_VERIFY_READER;
  static final String NULL_FIELD_INDICATOR_DOC = "Indicator to determine how the CSV Reader can determine if a field is null. Valid values are " + Joiner.on(", ").join(nullFieldIndicatorValues())
      + ". For more information see http://opencsv.sourceforge.net/apidocs/com/opencsv/enums/CSVReaderNullFieldIndicator.html.";
  static final String NULL_FIELD_INDICATOR_DEFAULT = CSVParser.DEFAULT_NULL_FIELD_INDICATOR.name();
  static final String KEY_FIELDS_DOC = "The fields that should be used as a key for the message.";
  static final String FIRST_ROW_AS_HEADER_DOC = "Flag to indicate if the fist row of data contains the header of the file.";
  static final boolean FIRST_ROW_AS_HEADER_DEFAULT = false;
  static final String CHARSET_DOC = "Character set to read wth file with.";
  static final String CHARSET_DEFAULT = Charset.defaultCharset().name();
  static final String PARSER_TIMESTAMP_DATE_FORMATS_DOC = "The date formats that are expected in the file. This is a list " +
      "of strings that will be used to parse the date fields in order. The most accurate date format should be the first " +
      "in the list. Take a look at the Java documentation for more info. " +
      "https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html";
  static final List<String> PARSER_TIMESTAMP_DATE_FORMATS_DEFAULT = Arrays.asList("yyyy-MM-dd' 'HH:mm:ss");
  static final String PARSER_TIMESTAMP_TIMEZONE_DOC = "The timezone that all of the dates will be parsed with.";
  static final String PARSER_TIMESTAMP_TIMEZONE_DEFAULT = "UTC";
  static final String SCHEMA_DOC = "Schema representaiton in json.";
  static final String INFER_SCHEMA_FROM_HEADER_DOC = "Flag to determine if the structSchema should be generated based on the header row.";
  static final String CASE_SENSITIVE_FIELD_NAMES_DOC = "Flag to determine if the field names in the header row should be treated as case sensitive.";

  public SpoolDirectoryConfig(Map<?, ?> parsedConfig) {
    super(getConf(), parsedConfig);
  }

  public static ConfigDef getConf() {
    return new ConfigDef()
        //DirectoryMonitorConfig
        .define(RECORD_PROCESSOR_CLASS_CONF, ConfigDef.Type.CLASS, ConfigDef.Importance.HIGH, RECORD_PROCESSOR_CLASS_DOC)

        //PollingDirectoryMonitorConfig
        .define(INPUT_PATH_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, INPUT_PATH_DOC)
        .define(FINISHED_PATH_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, FINISHED_PATH_DOC)
        .define(ERROR_PATH_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, ERROR_PATH_DOC)
        .define(INPUT_FILE_PATTERN_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, INPUT_FILE_PATTERN_DOC)
        .define(HALT_ON_ERROR_CONF, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH, HALT_ON_ERROR_DOC)
        .define(FILE_MINIMUM_AGE_MS_CONF, ConfigDef.Type.LONG, 0L, ConfigDef.Range.between(0L, Long.MAX_VALUE), ConfigDef.Importance.HIGH, FILE_MINIMUM_AGE_MS_DOC)

        //RecordProcessorConfig
        .define(BATCH_SIZE_CONF, ConfigDef.Type.INT, BATCH_SIZE_DEFAULT, ConfigDef.Importance.LOW, BATCH_SIZE_DOC)
        .define(TOPIC_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TOPIC_DOC)

        //CSVRecordProcessorConfig
        .define(SKIP_LINES_CONF, ConfigDef.Type.INT, SKIP_LINES_DEFAULT, ConfigDef.Importance.LOW, SKIP_LINES_DOC)
        .define(SEPARATOR_CHAR_CONF, ConfigDef.Type.INT, SEPARATOR_CHAR_DEFAULT, ConfigDef.Importance.MEDIUM, SEPARATOR_CHAR_DOC)
        .define(QUOTE_CHAR_CONF, ConfigDef.Type.INT, QUOTE_CHAR_DEFAULT, ConfigDef.Importance.MEDIUM, QUOTE_CHAR_DOC)
        .define(ESCAPE_CHAR_CONF, ConfigDef.Type.INT, ESCAPE_CHAR_DEFAULT, ConfigDef.Importance.MEDIUM, ESCAPE_CHAR_DOC)
        .define(STRICT_QUOTES_CONF, ConfigDef.Type.BOOLEAN, STRICT_QUOTES_DEFAULT, ConfigDef.Importance.LOW, STRICT_QUOTES_DOC)
        .define(IGNORE_LEADING_WHITESPACE_CONF, ConfigDef.Type.BOOLEAN, IGNORE_LEADING_WHITESPACE_DEFAULT, ConfigDef.Importance.LOW, IGNORE_LEADING_WHITESPACE_DOC)
        .define(IGNORE_QUOTATIONS_CONF, ConfigDef.Type.BOOLEAN, IGNORE_QUOTATIONS_DEFAULT, ConfigDef.Importance.LOW, IGNORE_QUOTATIONS_DOC)
        .define(KEEP_CARRIAGE_RETURN_CONF, ConfigDef.Type.BOOLEAN, KEEP_CARRIAGE_RETURN_DEFAULT, ConfigDef.Importance.LOW, KEEP_CARRIAGE_RETURN_DOC)
        .define(VERIFY_READER_CONF, ConfigDef.Type.BOOLEAN, VERIFY_READER_DEFAULT, ConfigDef.Importance.LOW, VERIFY_READER_DOC)
        .define(NULL_FIELD_INDICATOR_CONF, ConfigDef.Type.STRING, NULL_FIELD_INDICATOR_DEFAULT, ConfigDef.ValidString.in(nullFieldIndicatorValues()), ConfigDef.Importance.LOW, NULL_FIELD_INDICATOR_DOC)
        .define(KEY_FIELDS_CONF, ConfigDef.Type.LIST, Arrays.asList(), ConfigDef.Importance.HIGH, KEY_FIELDS_DOC)
        .define(FIRST_ROW_AS_HEADER_CONF, ConfigDef.Type.BOOLEAN, FIRST_ROW_AS_HEADER_DEFAULT, ConfigDef.Importance.HIGH, FIRST_ROW_AS_HEADER_DOC)
        .define(CHARSET_CONF, ConfigDef.Type.STRING, CHARSET_DEFAULT, ConfigDef.Importance.MEDIUM, CHARSET_DOC)
        .define(PARSER_TIMESTAMP_TIMEZONE_CONF, ConfigDef.Type.STRING, PARSER_TIMESTAMP_TIMEZONE_DEFAULT, ConfigDef.Importance.MEDIUM, PARSER_TIMESTAMP_TIMEZONE_DOC)
        .define(PARSER_TIMESTAMP_DATE_FORMATS_CONF, ConfigDef.Type.LIST, PARSER_TIMESTAMP_DATE_FORMATS_DEFAULT, ConfigDef.Importance.MEDIUM, PARSER_TIMESTAMP_DATE_FORMATS_DOC)
        .define(SCHEMA_CONF, Type.STRING, "", ConfigDef.Importance.MEDIUM, SCHEMA_DOC)
        .define(INFER_SCHEMA_FROM_HEADER_CONF, Type.BOOLEAN, false, ConfigDef.Importance.LOW, INFER_SCHEMA_FROM_HEADER_DOC)
        .define(CASE_SENSITIVE_FIELD_NAMES_CONF, Type.BOOLEAN, false, ConfigDef.Importance.LOW, CASE_SENSITIVE_FIELD_NAMES_DOC);


  }

  File getFile(String key) {
    String path = this.getString(key);
    File file = new File(path);
    Preconditions.checkState(file.isAbsolute(), "'%s' must be an absolute path.", key);
    return new File(path);
  }

  public File inputPath() {
    return getFile(INPUT_PATH_CONFIG);
  }

  public File finishedPath() {
    return getFile(FINISHED_PATH_CONFIG);
  }

  public File errorPath() {
    return getFile(ERROR_PATH_CONFIG);
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
    return this.getInt(SKIP_LINES_CONF);
  }

  char getChar(String key) {
    int intValue = this.getInt(key);
    return (char) intValue;
  }

  public char separatorChar() {
    return this.getChar(SEPARATOR_CHAR_CONF);
  }

  public char quoteChar() {
    return this.getChar(QUOTE_CHAR_CONF);
  }

  public char escapeChar() {
    return this.getChar(ESCAPE_CHAR_CONF);
  }

  public boolean ignoreLeadingWhitespace() {
    return this.getBoolean(IGNORE_LEADING_WHITESPACE_CONF);
  }

  public boolean ignoreQuotations() {
    return this.getBoolean(IGNORE_QUOTATIONS_CONF);
  }

  public boolean strictQuotes() {
    return this.getBoolean(STRICT_QUOTES_CONF);
  }

  public boolean keepCarriageReturn() {
    return this.getBoolean(KEEP_CARRIAGE_RETURN_CONF);
  }

  public boolean verifyReader() {
    return this.getBoolean(VERIFY_READER_CONF);
  }

  public CSVReaderNullFieldIndicator nullFieldIndicator() {
    String value = this.getString(NULL_FIELD_INDICATOR_CONF);
    return CSVReaderNullFieldIndicator.valueOf(value);
  }

  public boolean firstRowAsHeader() {
    return this.getBoolean(FIRST_ROW_AS_HEADER_CONF);
  }

  public List<String> keyFields() {
    return this.getList(KEY_FIELDS_CONF);
  }

  public Charset charset() {
    String value = this.getString(CHARSET_CONF);
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
    List<String> formats = this.getList(PARSER_TIMESTAMP_DATE_FORMATS_CONF);

    for (String s : formats) {
      results.add(new SimpleDateFormat(s));
    }

    return results.toArray(new SimpleDateFormat[results.size()]);
  }

  public TimeZone parserTimestampTimezone() {
    String value = this.getString(PARSER_TIMESTAMP_TIMEZONE_CONF);
    return TimeZone.getTimeZone(value);
  }

  public Class<RecordProcessor> recordProcessor() {
    //TODO: Comeback and verifiy this is of the proper interface.
    return (Class<RecordProcessor>) this.getClass(RECORD_PROCESSOR_CLASS_CONF);
  }

  public SchemaConfig schemaConfig() {
    String fieldContent = this.getString(SCHEMA_CONF);

    Preconditions.checkState(null != fieldContent && !fieldContent.isEmpty(), "%s must be configured with a valid structSchema.", SCHEMA_CONF);

    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.enable(SerializationFeature.WRITE_ENUMS_USING_TO_STRING);
    objectMapper.enable(DeserializationFeature.READ_ENUMS_USING_TO_STRING);
    try {
      return objectMapper.readValue(fieldContent, SchemaConfig.class);
    } catch (IOException e) {
      throw new ConnectException("Could not parse schemaConfig json", e);
    }
  }

  public boolean inferSchemaFromHeader() {
    return this.getBoolean(INFER_SCHEMA_FROM_HEADER_CONF);
  }

  public boolean caseSensitiveFieldNames() {
    return this.getBoolean(CASE_SENSITIVE_FIELD_NAMES_CONF);
  }

}
