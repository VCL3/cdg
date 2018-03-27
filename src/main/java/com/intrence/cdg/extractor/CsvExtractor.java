package com.intrence.cdg.extractor;

import com.fasterxml.jackson.databind.JsonNode;
import com.intrence.cdg.net.FetchRequest;
import com.intrence.cdg.util.Constants;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.StringReader;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Arrays;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.intrence.cdg.util.Constants.QUOTE;

public class CsvExtractor extends BaseExtractor<CSVRecord>{

    private final static Logger LOGGER = Logger.getLogger(CsvExtractor.class);

    private CSVParser csvParser;
    private List<CSVRecord> csvRecords;
    private char delimiter;

    public CsvExtractor(String source, String content, String url, String headers) {
        this(source, content, url, Constants.COMMA.charAt(0), headers);
    }

    public CsvExtractor(String source, String content, String url, char delimiter, String headers) {
        this(source, content, url, delimiter, QUOTE, headers);
    }

    public CsvExtractor(String source, String content, String url, char delimiter, Character quote, String headers) {
        super(source, content, url);
        csvRecords = new ArrayList<>();
        try {
            this.delimiter = delimiter;
            CSVFormat format = CSVFormat.DEFAULT.withDelimiter(delimiter)
                                                .withHeader(createHeaders(headers))
                                                .withQuote(quote);
            this.csvParser = new CSVParser(new StringReader(content), format);
            this.csvRecords = csvParser.getRecords();
        } catch (Exception e) {
            LOGGER.error(String.format("Exception=CSVParserError while initializing CSVExtractor for source=%s, url=%s", source, url), e);
            throw new IllegalArgumentException(e.getMessage(), e.getCause());
        }
    }

    public List<CSVRecord> getContentRecords() {
        return csvRecords;
    }

    public CSVRecord getContentRecord(int index) {
        return csvRecords.get(index);
    }

    @Override
    public CSVRecord getContentTree() {
        throw new NotImplementedException("Not implemented for CSVExtractor");
    }

    @Override
    public CSVRecord getContentTree(String content) {
        throw new NotImplementedException("Not implemented for CSVExtractor");
    }

    @Override
    public <T> Set<T> extractEntities(Function<CSVRecord, T> extractEntityFunction) {
        return csvRecords.stream()
                .map(extractEntityFunction)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<FetchRequest> extractLinks(String pageType, String extractionPattern) {
        return new HashSet<>();
    }

    @Override
    public String extractField(String rule, CSVRecord csvRecord) {
        String fieldValue = null;
        try {
            fieldValue = csvRecord.get(rule);
        } catch (Exception e) {
            LOGGER.error(String.format("Exception=CSVParserError  while extracting the field using rule=%s, " +
                    "for source=%s, url=%s, errorMessage=%s", rule, source, url, e.getMessage()));
        }
        return fieldValue;
    }

    @Override
    public List<String> extractFields(String rule, CSVRecord csvRecord) {
        List<String> fieldValues = new ArrayList<>();
        try {
            String data = csvRecord.get(rule);
            fieldValues = Arrays.asList(data.split(Constants.COMMA));
        } catch (Exception e) {
            LOGGER.error(String.format("Exception=CSVParserError  while extracting the field using rule=%s, " +
                    "for source=%s, url=%s, errorMessage=%s", rule, source, url, e.getMessage()));
        }
        return fieldValues;
    }

    @Override
    public CSVRecord extractFieldObject(String rule, CSVRecord csvRecord) {
        throw new NotImplementedException("Not implemented for CSVExtractor");
    }

    @Override
    public CSVRecord[] extractFieldObjects(String rule, CSVRecord csvRecord) {
        throw new NotImplementedException("Not implemented for CSVExtractor");
    }

    @Override
    public List<Map<String, String>> extractRepeatedFields(JsonNode repeatedRules, CSVRecord csvRecord) {
        throw new NotImplementedException("Not implemented for CSVExtractor");
    }

    private static String[] createHeaders(String headers) {
        if (StringUtils.isNotBlank(headers)) {
            return headers.split(Constants.COMMA);
        }
        throw new IllegalArgumentException("CSV headers can't be null or empty !!");
    }

    public static List<String> getValues(CSVRecord record) {
        if(record!=null) {
            List<String> values = new ArrayList<>();
            Iterator recordIterator = record.iterator();
            while (recordIterator.hasNext()) {
                values.add(recordIterator.next().toString());
            }
            return values;
        }
        return null;
    }
}
