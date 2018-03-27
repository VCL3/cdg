package com.intrence.cdg.crawler;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.SortedMap;
import java.util.Collections;
import java.net.URISyntaxException;
import java.net.URLDecoder;

/**
 * - Covert the scheme and host to lowercase (done by java.net.URL)
 * - Normalize the path (done by java.net.URI)
 * - Add the port number.
 * - Remove trailing slash.
 * - Sort the query string params.
 * - Removing the Fragment part.(Url part after '#')
 * - Remove some query string params like "utm_*" and "*session*".
 */
public class NormalizeURL {
    private static final Logger LOGGER = Logger.getLogger(NormalizeURL.class);

    //http://stackoverflow.com/questions/2993649/how-to-normalize-a-url-in-java
    public static String normalize(String taintedURL) throws URISyntaxException, MalformedURLException {
        //removing fragment part from the url to avoid risk of crawling same url
        if (taintedURL.contains("#")) {
            taintedURL = taintedURL.split("#")[0];
        }

        taintedURL = removeJunkParams(taintedURL);
        final SortedMap<String, List<String>> params = createSortedParameterMap(new URL(taintedURL).getQuery());
        try {
            taintedURL = URLDecoder.decode(taintedURL, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            LOGGER.debug("unsupported encoding exception for the url="+taintedURL);
        }
        taintedURL = taintedURL.replaceAll("\\s+","+");
        final URL url = new URL(taintedURL);
        String path = url.getPath().replace("/$", "");
        final int port = url.getPort();

        URIBuilder builder;
        if (params != null) {
            // Some params are only relevant for user tracking, so remove the most commons ones.
            for (Iterator<String> i = params.keySet().iterator(); i.hasNext(); ) {
                final String key = i.next();
                if (key.startsWith("utm_") || key.contains("session")) {
                    i.remove();
                }
            }
            builder = canonicalize(params);
        } else {
            builder = new URIBuilder();
        }

        if(port != -1 && port != 80)
            builder.setPort(port);
        return builder.setScheme(url.getProtocol().toLowerCase())
                .setHost(url.getHost().toLowerCase())
                .setPath(path).build().normalize().toString();
    }

    private static String removeJunkParams(String url){
        ImmutableSet<String> junkParamsSet = ImmutableSet.of("amp;");
        for(String junkParam : junkParamsSet){
            if(url.contains(junkParam)) {
                url = url.replaceAll(junkParam, "");
            }
        }
        return url;
    }

    /**
     * Takes a query string, separates the constituent name-value pairs, and
     * stores them in a SortedMap ordered by lexicographical order.
     *
     * @return Null if there is no query string.
     */
    protected static SortedMap<String, List<String>> createSortedParameterMap(final String queryString) {
        if (StringUtils.isBlank(queryString)) {
            return null;
        }

        final String[] pairs = queryString.split("&");
        final Map<String, List<String>> params = new HashMap<>(pairs.length);

        for (final String pair : pairs) {
            if (pair.length() < 1) {
                continue;
            }

            String[] tokens = pair.split("=", 2);
            for (int j = 0; j < tokens.length; j++) {
                try {
                    tokens[j] = URLDecoder.decode(tokens[j], "UTF-8");
                } catch (UnsupportedEncodingException ex) {
                    LOGGER.debug("error in decoding query params");
                }
            }
            switch (tokens.length) {
                case 1: {
                    if (pair.charAt(0) == '=') {
                        putParamInMap(params,"", tokens[0]);
                    } else {
                        putParamInMap(params,tokens[0], "");
                    }
                    break;
                }
                case 2: {
                    putParamInMap(params, tokens[0], tokens[1]);
                    break;
                }
            }
        }

        return new TreeMap<>(params);
    }

    private static void putParamInMap(Map<String,List<String>> params, String key, String value){
        if(!params.containsKey(key))
            params.put(key, new ArrayList<String>());
        params.get(key).add(value);
        Collections.sort(params.get(key));
    }

    /**
     * Canonicalize the query string.
     *
     * @param sortedParamMap Parameter name-value pairs in lexicographical order.
     * @return Canonical form of query string.
     */
    private static URIBuilder canonicalize(final SortedMap<String, List<String>> sortedParamMap) {
        URIBuilder builder = new URIBuilder();
        if (sortedParamMap == null || sortedParamMap.isEmpty()) {
            return builder;
        }

        final Iterator<Map.Entry<String, List<String>>> iter = sortedParamMap.entrySet().iterator();

        while (iter.hasNext()) {
            final Map.Entry<String, List<String>> pair = iter.next();

            String encodedKey = pair.getKey();
            for(String value: pair.getValue()) {
                builder.addParameter(encodedKey, value);
            }

        }

        return builder;
    }
}
