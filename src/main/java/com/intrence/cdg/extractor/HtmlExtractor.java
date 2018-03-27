/**
 * Created by wliu on 11/8/17.
 */
package com.intrence.cdg.extractor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.intrence.cdg.net.FetchRequest;
import com.intrence.cdg.crawler.CrawlConfig;
import com.intrence.cdg.crawler.CrawlablePage;
import com.intrence.cdg.parser.ParserFactory;
import com.intrence.cdg.url.UrlHelper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.htmlcleaner.CleanerProperties;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.PrettyXmlSerializer;
import org.htmlcleaner.TagNode;
import org.htmlcleaner.XmlSerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HtmlExtractor extends BaseExtractor<TagNode> {

    private final static Logger LOGGER = Logger.getLogger(HtmlExtractor.class);
    private final static String RULES_NODE = "/rules";
    private final static String XML_ANCESTOR_NODE = "/xml_ancestor";
    private final static XmlSerializer xmlSerializer = initXmlSerializer();
    private TagNode domTree;

    private static XmlSerializer initXmlSerializer() {
        HtmlCleaner cleaner = new HtmlCleaner();
        CleanerProperties cleanerProperties = cleaner.getProperties();
        cleanerProperties.setOmitXmlDeclaration(true);
        return new PrettyXmlSerializer(cleanerProperties);
    }

    public HtmlExtractor(String source, String content, String url) {
        super(source, content, url);
        createContentTree(content);
    }

    private void createContentTree(String content) {
        domTree = getContentTree(content);
    }

    @Override
    public TagNode getContentTree(String content) {
        CleanerProperties cleanerProperties = new CleanerProperties();
        cleanerProperties.setCharset("UTF-8");
        //replace <br> tags with space prefixed <br> tag so that during field extraction we will get data with spaces
        content = content.replaceAll("<br>", " <br>").replaceAll("<br/>", " <br/>");
        return new HtmlCleaner(cleanerProperties).clean(content);
    }

    @Override
    public TagNode getContentTree() {
        return domTree;
    }

    @Override
    public <T> Set<T> extractEntities(Function<TagNode, T> extractEntityFunction) {
        Set<T> entities = new HashSet<>();
        JsonNode rules = ParserFactory.getParserConfig(source, url);
        if (rules != null) {
            JsonNode parsingRules = rules.at(RULES_NODE);
            if (!parsingRules.at(XML_ANCESTOR_NODE).isMissingNode()) {
                TagNode[] tagNodes = extractFieldObjects(parsingRules.at(XML_ANCESTOR_NODE).asText(), domTree);
                for (TagNode tagNode : tagNodes) {
                    T entity = extractEntityFunction.apply(tagNode);
                    if(entity!=null) {
                        entities.add(entity);
                    }
                }
            } else {
                T entity = extractEntityFunction.apply(domTree);
                if (entity != null) {
                    entities.add(entity);
                }
            }
        }
        return entities;
    }

    @Override
    public Set<FetchRequest> extractLinks(String pageType, String extractionPattern) {
        return extractLinks(pageType, extractionPattern,false);
    }

    public Set<FetchRequest> extractLinks(String pageType, String extractionPattern, boolean removeQueryParameters) {
        CrawlConfig crawlConfig = ParserFactory.getCrawlConfig(source);

        // if no outlink configured return empty set
        if (crawlConfig.getCrawlablePages() ==  null) {
            throw new RuntimeException("something wrong, no crawlable pages defined in crawlConfig");
        }

        Pattern pattern = Pattern.compile(extractionPattern);
        Matcher matcher = pattern.matcher(this.content);
        Set<FetchRequest> newReqs = new HashSet<>();

        while (matcher.find()) {
            try {
                String outLink = matcher.group(1);
                if (StringUtils.isNotBlank(outLink)) {
                    outLink = UrlHelper.formatUrl(outLink.trim(), url);
                } else {
                    continue;
                }
                boolean isUrlMatched = false;

                //Filter the extracted url based on the crawl config graph.
                if (CollectionUtils.isNotEmpty(crawlConfig.getCrawlGraph().get(pageType))) {
                    for (String destination : crawlConfig.getCrawlGraph().get(pageType)) {
                        CrawlablePage destPage = crawlConfig.getCrawlablePages().get(destination);
                        if (destPage != null && UrlHelper.isUrlMatched(destPage.getUrlPatterns(), outLink)) {
                            outLink = removeQueryParameters ? UrlHelper.removeQueryParameters(outLink) : outLink;
                            newReqs.add(new FetchRequest(outLink, destPage.getPriority()));
                            isUrlMatched = true;
                        }
                    }
                }

                if (!isUrlMatched) {
                    LOGGER.debug(String.format("rejecting the url=%s as url doesn't match any allowed pattern from " +
                            "source_url=%s", outLink, url));
                }
            } catch (Exception ex){
                LOGGER.warn(String.format("error while extracting url from the content of page with url=%s", url), ex);
            }
        }
        return newReqs;
    }

    @Override
    public String extractField(String rule, TagNode domTree) {

        String field = null;
        try {
            List<String> fields = extractFields(rule, domTree);
            if (CollectionUtils.isNotEmpty(fields)) {
                field = fields.get(0);
            }
        } catch(Exception e) {
            LOGGER.error(String.format("Exception=HtmlParserError  while extracting the field using xpath=%s, " +
                    "for source=%s, url=%s", rule, source, url), e);
        }
        return field;
    }

    @Override
    public List<String> extractFields(String rule, TagNode domTree) {

        ArrayList<String> fieldsSet = new ArrayList<>();

        try {
            Object[] fields = extractObjects(rule, domTree);
            if (fields == null) {
                return null;
            }
            for (Object field : fields) {
                String fieldData;
                if (field instanceof TagNode) {
                    TagNode fieldNode = (TagNode) field;
                    //replace continuous spaces with one space.
                    fieldData = fieldNode.getText().toString().replaceAll(" {1,}", " ");
                } else if (field instanceof String) {
                    fieldData = (String) field;
                } else if (field instanceof StringBuilder){
                    fieldData = field.toString();
                } else {
                    continue;
                }
                if (StringUtils.isNotBlank(fieldData)) {
                    fieldsSet.add(decodeHtmlChars(fieldData.trim()));
                }
            }
        } catch (Exception e) {
            LOGGER.error(String.format("Exception=HtmlParserError  while extracting the field using xpath=%s, " +
                    "for source=%s, url=%s", rule, source, url), e);
        }
        return fieldsSet;
    }

    @Override
    public TagNode extractFieldObject(String rule, TagNode domTree) {

        TagNode[] fieldObjects = extractFieldObjects(rule, domTree);

        return ArrayUtils.isEmpty(fieldObjects) ? null : fieldObjects[0];
    }

    @Override
    public TagNode[] extractFieldObjects(String rule, TagNode domTree) {

        try {
            if(StringUtils.isBlank(rule)) {
                return null;
            }

            Object[] fields = extractObjects(rule, domTree);
            if (fields != null && fields.length > 0) {
                List<TagNode> tagNodes = new LinkedList<>();
                for (Object field : fields) {
                    if (field instanceof TagNode) {
                        tagNodes.add((TagNode)field);
                    }
                }
                if (tagNodes.size() > 0) {
                    return tagNodes.toArray(new TagNode[tagNodes.size()]);
                }
            }
        } catch (Exception e) {
            LOGGER.error(String.format("Exception=HtmlParserError  while extracting the field using xpath=%s, " +
                    "for source=%s, url=%s", rule, source, url), e);
        }

        return null;
    }

    @Override
    public List<Map<String, String>> extractRepeatedFields(JsonNode repeatedRules, TagNode domTree) {

        if(repeatedRules!=null&&repeatedRules.has(ANCESTOR_PATH)) {
            String ancestorXpath = repeatedRules.get(ANCESTOR_PATH).asText();
            try {
                TagNode[] fields = extractFieldObjects(ancestorXpath, domTree);
                if(fields == null)
                    return null;
                List<Map<String, String>> repeatedFields = new ArrayList<Map<String, String>>();
                for (TagNode fieldNode : fields) {
                    Map<String, String> simpleFields = new HashMap<String, String>();
                    Iterator keyIterator = repeatedRules.fieldNames();
                    while (keyIterator.hasNext()) {
                        String key = (String) keyIterator.next();
                        JsonNode innerField = repeatedRules.get(key);
                        if (innerField.getNodeType().equals(JsonNodeType.STRING)) {
                            String field = extractField(innerField.asText(), fieldNode);
                            if(field != null)
                                simpleFields.put(key, field);
                        }
                    }
                    repeatedFields.add(simpleFields);
                }

                return repeatedFields;

            } catch (Exception ex) {
                LOGGER.error(String.format("Exception=HtmlParserError while extracting the ancestorXpath=%s for the source=%s",
                        ancestorXpath, this.source), ex);
            }
        }
        return null;

    }


    private Object[] evaluateXpath(String xpathStr, TagNode domTree) throws Exception{
        //Ex Regex:- //div[@class='below_the_fold']//div[contains(@id,'reviewSelector')]/span[@class='reviewText']
        String regex = "(.*?\\[)(contains|starts-with)\\(@(.*?),'(.*?)'\\)\\](.*)";
        Matcher matcher = getStringMatcher(regex,xpathStr);
        if(matcher.find()) {
            String xpathBeforeFunc = matcher.group(1); //from above ex:  //div[@class='below_the_fold']//div[
            String xpathFuncType = matcher.group(2); // contains
            String attrName = matcher.group(3); // id
            String attrValue = matcher.group(4); // "reviewSelector"
            String lastPart = matcher.group(5); // /span[@class='reviewText']
            String xpathPart1 = xpathBeforeFunc+"@"+attrName+"]"; //div[@class='below_the_fold']//div[@id]
            List<TagNode> tagNodes = new ArrayList<TagNode>();
            Object[] fields = domTree.evaluateXPath(xpathPart1);
            for (Object fieldNode : fields) {
                try {
                    if(!(fieldNode instanceof TagNode))
                        continue;
                    TagNode innerTagNode = (TagNode) fieldNode;
                    boolean requiredNode = false;
                    if (xpathFuncType.equals("contains")) {
                        if (innerTagNode.getAttributes().get(attrName).contains(attrValue)) {
                            requiredNode = true;
                        }
                    } else if (xpathFuncType.equals("starts-with")) {
                        if (innerTagNode.getAttributes().get(attrName).startsWith(attrValue)) {
                            requiredNode = true;
                        }
                    }

                    if (requiredNode) { //if we found desired node
                        if (!StringUtils.isBlank(lastPart)) { // apply remaining part of the xpathStr rule to this field
                            Object[] fi = evaluateXpath(lastPart, innerTagNode); //recursively check for other spath functions in last part
                            for (Object fids : fi) {
                                if(!(fids instanceof TagNode))
                                    continue;
                                tagNodes.add((TagNode) fids);
                            }
                        } else
                            tagNodes.add(innerTagNode);
                    }
                }catch (Exception ex){
                    LOGGER.error(String.format("Exception=HtmlParserError XpathEvaluation error for source=%s, url=%s",
                            source, url),ex);
                }

            }
            return tagNodes.toArray();
        } else {
            return domTree.evaluateXPath(xpathStr);
        }
    }

    private String decodeHtmlChars(String htmlEncodedString) {
        return StringEscapeUtils.unescapeHtml4(htmlEncodedString);
    }

    private Object[] extractObjects(String rule, TagNode domTree) {

        try {
            if(StringUtils.isBlank(rule)) {
                return null;
            }
            String[] xpaths = rule.split("\\|"); //check for two xpaths seperated by an OR(|) operator
            for(String xpathStr : xpaths){
                Object[] fields = evaluateXpath(xpathStr, domTree);
                if (fields!=null && fields.length>0) {
                    return fields;
                }
            }
        } catch (Exception e) {
            LOGGER.error(String.format("Exception=HtmlParserError  while extracting the field using xpath=%s, " +
                    "for source=%s, url=%s", rule, source, url), e);
        }
        return null;
    }

    @Override
    public String getNodeString(TagNode node)
    {
        return xmlSerializer.getAsString(node);
    }
}