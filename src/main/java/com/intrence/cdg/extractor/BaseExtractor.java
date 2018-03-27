/**
 * Created by wliu on 11/8/17.
 */
package com.intrence.cdg.extractor;

import com.google.common.base.Joiner;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class BaseExtractor<N> implements Extractor<N> {

    protected static final String ANCESTOR_PATH = "ancestor";
    private final static Logger LOGGER = Logger.getLogger(BaseExtractor.class);

    protected String source;
    protected String url;
    protected String content;

    public BaseExtractor(String source, String content, String url) {
        if (StringUtils.isBlank(content)) {
            LOGGER.error(String.format("Exception=ParserError while initializing Extractor for source=%s, url=%s, " +
                    "errorCause=Content can't be null", source, url));
            throw new IllegalArgumentException("Content can't be null or empty !!");
        }
        this.source = source;
        this.url = url;
        this.content = content;
    }

    @Override
    public String getContentString() {
        return content;
    }

    @Override
    public String extractAndConcatenateFields(String rule, N contentTree) {
        List<String> data = extractFields(rule, contentTree);

        return CollectionUtils.isEmpty(data) ?
                null :
                Joiner.on(",").join(data);
    }

    protected Matcher getStringMatcher(String regex, String text){
        if(regex == null || text == null)
            return null;
        Pattern pattern = Pattern.compile(regex);
        return pattern.matcher(text);
    }

    @Override
    public String getNodeString(N node){
        throw new NotImplementedException("");
    }
}