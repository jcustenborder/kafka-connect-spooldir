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
import java.util.List;
import java.util.Map;

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
  //  static final String[] nullFieldIndicatorValues;
  static final String SKIP_LINES_DOC = "Number of lines to skip in the beginning of the file.";
  static final int SKIP_LINES_DEFAULT = CSVReader.DEFAULT_SKIP_LINES;
  static final String SEPARATOR_CHAR_DOC = "Separator character.";
  static final int SEPARATOR_CHAR_DEFAULT = (int) CSVParser.DEFAULT_SEPARATOR;
  static final String QUOTE_CHAR_DOC = "Quote character.";
  static final int QUOTE_CHAR_DEFAULT = (int) CSVParser.DEFAULT_QUOTE_CHARACTER;
  static final String ESCAPE_CHAR_DOC = "Escape character.";
  static final int ESCAPE_CHAR_DEFAULT = (int) CSVParser.DEFAULT_ESCAPE_CHARACTER;
  static final String STRICT_QUOTES_DOC = "strict quotes.";
  static final boolean STRICT_QUOTES_DEFAULT = CSVParser.DEFAULT_STRICT_QUOTES;
  static final String IGNORE_LEADING_WHITESPACE_DOC = "ignore_leading_whitespace character.";
  static final boolean IGNORE_LEADING_WHITESPACE_DEFAULT = CSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE;
  static final String IGNORE_QUOTATIONS_DOC = "ignore_quotations character.";
  static final boolean IGNORE_QUOTATIONS_DEFAULT = CSVParser.DEFAULT_IGNORE_QUOTATIONS;
  static final String KEEP_CARRIAGE_RETURN_DOC = "Flag to determine if the carriage return at the end of the line should be maintained.";
  static final boolean KEEP_CARRIAGE_RETURN_DEFAULT = CSVReader.DEFAULT_KEEP_CR;
  static final String VERIFY_READER_DOC = "Flag to determine if the reader should be verified.";
  static final boolean VERIFY_READER_DEFAULT = CSVReader.DEFAULT_VERIFY_READER;
  static final String NULL_FIELD_INDICATOR_DOC = "Flag to determine if the reader should be verified. Valid values are " + Joiner.on(", ").join(nullFieldIndicatorValues());
  static final String NULL_FIELD_INDICATOR_DEFAULT = CSVParser.DEFAULT_NULL_FIELD_INDICATOR.name();
  static final String KEY_FIELDS_DOC = "The fields that should be used as a key for the message.";
  static final String FIRST_ROW_AS_HEADER_DOC = "Flag to indicate if the fist row of data contains the header of the file.";
  static final boolean FIRST_ROW_AS_HEADER_DEFAULT = false;
  static final String CHARSET_DOC = "Character set to read wth file with.";
  static final String CHARSET_DEFAULT = Charset.defaultCharset().name();

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


}
