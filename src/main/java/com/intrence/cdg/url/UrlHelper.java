/**
 * Created by wliu on 11/8/17.
 */
package com.intrence.cdg.url;

import com.intrence.cdg.net.FetchRequest;
import com.intrence.cdg.crawler.CrawlConfig;
import com.intrence.cdg.crawler.CrawlablePage;
import com.intrence.cdg.parser.ParserFactory;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UrlHelper {

    private static final String HASH = "#";
    private static final String HTTP = "http";
    private static final String FORWARD_SLASH = "/";
    private static final String EMPTY_STRING = "";
    public static final String QUERY_PARAMETERS_REGEX = "(\\?.*)";

    public static Matcher getStringMatcher(String regex, String text){
        if (regex == null || text == null) {
            return null;
        }
        Pattern pattern = Pattern.compile(regex);
        return pattern.matcher(text);
    }

    public static boolean isUrlMatched(Set<String> urlPatterns, String urlStr) {
        if (urlPatterns!=null) {
            for (String urlPattern : urlPatterns) {
                Matcher matcher = getStringMatcher(urlPattern, urlStr);
                if (matcher!=null && matcher.find()) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isUrlMatched(String urlPattern, String urlStr) {
        Matcher matcher = getStringMatcher(urlPattern, urlStr);
        if (matcher!=null && matcher.find()) {
            return true;
        }
        return false;
    }

    public static String getPageTypeFromUrl(String source, String url){
        String pageType = null;
        int matchLength = 0;
        CrawlConfig crawlConfig = ParserFactory.getCrawlConfig(source);
        for (Map.Entry<String, CrawlablePage> entry: crawlConfig.getCrawlablePages().entrySet()) {
            CrawlablePage page = entry.getValue();
            for (String pagePattern: page.getUrlPatterns()) {
                if (isUrlMatched(pagePattern, url)) {
                    if (pagePattern.length() > matchLength) {
                        pageType = entry.getKey();
                        matchLength = pagePattern.length();
                    }
                }
            }
        }
        return pageType;
    }

    public static String formatUrl(String urlToFormat, String url) throws MalformedURLException {
        URL originalUrl;
        originalUrl = new URL(url);

        if (urlToFormat.contains(HASH)) {
            String splitUrlToFormat[] = urlToFormat.split(HASH);
            if (ArrayUtils.isNotEmpty(splitUrlToFormat)) {
                urlToFormat = splitUrlToFormat[0];
            } else {
                urlToFormat = EMPTY_STRING;
            }
        }

        //check for relative urls paths and convert them into absolute urls
        if (!urlToFormat.startsWith(HTTP)) {
            URL urlObj = null;
            if (urlToFormat.startsWith(FORWARD_SLASH)) {
                urlObj = new URL(originalUrl.getProtocol(), originalUrl.getHost(), urlToFormat);
            } else {
                urlObj = new URL(originalUrl.getProtocol(), originalUrl.getHost(), "/" + urlToFormat);
            }

            urlToFormat = urlObj.toString();
        }
        return urlToFormat;
    }

    public static FetchRequest checkAndGetCrawlableRequest(String source, String url) {
        String pageType = getPageTypeFromUrl(source, url);
        FetchRequest fetchRequest = null;
        if (StringUtils.isNotBlank(pageType)) {
            CrawlConfig crawlConfig = ParserFactory.getCrawlConfig(source);
            fetchRequest = new FetchRequest(url, crawlConfig.getCrawlablePages().get(pageType).getPriority());
        }
        return fetchRequest;
    }

    public static String removeQueryParameters(final String url) {
        if (StringUtils.isNotBlank(url)) {
            return url.replaceAll(QUERY_PARAMETERS_REGEX, "");
        }
        return url;
    }
}
