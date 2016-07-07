package io.confluent.kafka.connect.source.io.processing;

import com.google.common.base.Joiner;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import org.apache.kafka.common.config.ConfigDef;

import java.io.Reader;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings("WeakerAccess")
public class CSVRecordProcessorConfig extends RecordProcessorConfig {
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
  public static final String CHARSET_CONF = "charset";
  public static final String PARSER_TIMESTAMP_DATE_FORMATS_CONF = "parser.timestamp.date.formats";
  public static final String PARSER_TIMESTAMP_TIMEZONE_CONF = "parser.timestamp.timezone";

  //  static final String[] nullFieldIndicatorValues;
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
  final Pattern fieldPrefixPatten = Pattern.compile("(\\d+)\\.");

  public CSVRecordProcessorConfig(Map<?, ?> originals) {
    super(getConf(), originals);
  }

  static String[] nullFieldIndicatorValues() {
    String[] result = new String[CSVReaderNullFieldIndicator.values().length];
    for (int i = 0; i < CSVReaderNullFieldIndicator.values().length; i++) {
      result[i] = CSVReaderNullFieldIndicator.values()[i].name();
    }
    return result;
  }

  public static ConfigDef getConf() {
    return RecordProcessorConfig.getConf()
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
        .define(KEY_FIELDS_CONF, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, KEY_FIELDS_DOC)
        .define(FIRST_ROW_AS_HEADER_CONF, ConfigDef.Type.BOOLEAN, FIRST_ROW_AS_HEADER_DEFAULT, ConfigDef.Importance.HIGH, FIRST_ROW_AS_HEADER_DOC)
        .define(CHARSET_CONF, ConfigDef.Type.STRING, CHARSET_DEFAULT, ConfigDef.Importance.MEDIUM, CHARSET_DOC)
        .define(PARSER_TIMESTAMP_TIMEZONE_CONF, ConfigDef.Type.STRING, PARSER_TIMESTAMP_TIMEZONE_DEFAULT, ConfigDef.Importance.MEDIUM, PARSER_TIMESTAMP_TIMEZONE_DOC)
        .define(PARSER_TIMESTAMP_DATE_FORMATS_CONF, ConfigDef.Type.LIST, PARSER_TIMESTAMP_DATE_FORMATS_DEFAULT, ConfigDef.Importance.MEDIUM, PARSER_TIMESTAMP_DATE_FORMATS_DOC)
        ;
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
        .withFieldAsNull(nullFieldIndicator())
        ;
  }

  public CSVReaderBuilder createCSVReaderBuilder(Reader reader, CSVParser parser) {
    return new CSVReaderBuilder(reader)
        .withCSVParser(parser)
        .withKeepCarriageReturn(this.keepCarriageReturn())
        .withSkipLines(this.skipLines())
        .withVerifyReader(this.verifyReader())
        .withFieldAsNull(nullFieldIndicator())
        ;
  }

  public List<CSVFieldConfig> fields() {
    List<CSVFieldConfig> fields = new ArrayList<>();

    Set<String> prefixes = new HashSet<>();

    Map<String, Object> fieldOriginals = this.originalsWithPrefix("fields.");

    for (Map.Entry<String, Object> mapEntry : fieldOriginals.entrySet()) {
      Matcher matcher = fieldPrefixPatten.matcher(mapEntry.getKey());
      if (matcher.find()) {
        prefixes.add(matcher.group(0));
      }
    }

    for (String prefix : prefixes) {
      Matcher matcher = fieldPrefixPatten.matcher(prefix);
      if (matcher.find()) {
        int index = Integer.parseInt(matcher.group(1));
        String originalPrefix = String.format("fields.%s", matcher.group(0));
        Map<String, Object> prefixOriginals = this.originalsWithPrefix(originalPrefix);
        CSVFieldConfig fieldConfig = new CSVFieldConfig(prefixOriginals, index);
        fields.add(fieldConfig);
      }
    }

    Collections.sort(fields, new Comparator<CSVFieldConfig>() {
      @Override
      public int compare(CSVFieldConfig o1, CSVFieldConfig o2) {
        return Integer.compare(o1.index, o2.index);
      }
    });

    return fields;
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

}
