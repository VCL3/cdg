package com.intrence.cdg.parser;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ParserUtils {

    private static final Logger LOGGER = Logger.getLogger(ParserUtils.class);
    private static final String CLEAN_STRING_PATTERN = "[\\t\\n\\r\\s]";

    public static Map<String, List<String>> extractQueryParams(URL url) {
        String[] queryParams = (url.getQuery()!=null) ? url.getQuery().split("&") : new String[]{};
        Map<String, List<String>> keyValuePairs = new LinkedHashMap<>();
        for(String param: queryParams) {
            int idx = param.indexOf("=");
            String key = (idx > 0) ? param.substring(0, idx) : param;
            String value = ((idx > 0) && (param.length() > idx+1))? param.substring(idx+1) : null;
            if (!keyValuePairs.containsKey(key)) {
                keyValuePairs.put(key, new ArrayList<String>());
            }
            keyValuePairs.get(key).add(value);
        }
        return keyValuePairs;
    }
    
    public static String replaceQueryParams(URL url, Map<String, List<String>> newParams) throws URISyntaxException, MalformedURLException {
        URIBuilder builder = new URIBuilder()
         .setScheme(url.getProtocol())
         .setHost(url.getHost())
         .setPort(url.getPort())
         .setPath(url.getPath());
        
        for (String key: newParams.keySet()) {
            List<String> values = newParams.get(key);
            for (String value : values) {
                builder.addParameter(key, value);
            }            
        }
        
        return builder.build().toString();
    }
    
    public static String checkAndEncodeUrl(String urlString) throws MalformedURLException, URISyntaxException {
        URL url = new URL(urlString);
        Map<String, List<String>> queryParams = extractQueryParams(url);        
        return replaceQueryParams(url, queryParams);
    }

    public static String cleanString(String data) {
        return StringUtils.isBlank(data) ? data : data.replaceAll(CLEAN_STRING_PATTERN, "");
    }

    public static String readJsonFile(String filePath) {
        String jsonString = null;
        InputStream inputStream = null;
        try {
            StringBuilder jsonData = new StringBuilder();
            inputStream = ParserUtils.class.getClassLoader().getResourceAsStream(filePath);
            BufferedReader r = new BufferedReader(new InputStreamReader(inputStream));

            String line;
            while ((line = r.readLine()) != null) {
                jsonData.append(line);
            }
            jsonString = jsonData.toString();
        } catch (IOException e) {
            LOGGER.error(String.format("failed to read the file =%s", filePath), e);
        } finally {
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (IOException e) {
                LOGGER.error(String.format("failed to close inputStream for file =%s", filePath), e);
            }
        }
        return jsonString;
    }

}
