/**
 * Created by wliu on 11/8/17.
 */
package com.intrence.cdg.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.intrence.cdg.extractor.HtmlExtractor;
import com.intrence.cdg.net.FetchRequest;
import org.htmlcleaner.TagNode;

public class FarfetchParser extends ProductParser<TagNode> {

    public FarfetchParser(String source, JsonNode rules, FetchRequest req, String content) {
        super(source, rules, req, new HtmlExtractor(source, content, req.getWorkRequest()));
    }

}
