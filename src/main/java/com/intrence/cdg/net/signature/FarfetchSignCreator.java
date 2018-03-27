package com.intrence.cdg.net.signature;


import com.intrence.config.collection.ConfigList;
import com.intrence.config.collection.ConfigMap;
import com.intrence.cdg.util.Constants;
import org.apache.http.client.utils.URIBuilder;

import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map;


public class FarfetchSignCreator implements SignCreator {

    private static final String ACCESS_TOKENS = "app_access_token";
    private static final String ACCESS_TOKEN_KEY = "access_token";
    private static final String SOURCE = "facebook";

    private final Map<Integer, String> appAccessTokens;
    private final int noOfTokens;

    public FarfetchSignCreator(ConfigMap sourcesConfigMap) {
        if (sourcesConfigMap == null || sourcesConfigMap.size() == 0) {
            throw new IllegalArgumentException("ConfigMap can't be null or empty !!");
        }
        ConfigMap authenticationMap = sourcesConfigMap.getMap(SOURCE).getMap(Constants.SIGNATURE_KEY);
        ConfigList accessTokens = authenticationMap.getList(ACCESS_TOKENS);
        ListIterator listIterator = accessTokens.listIterator();
        int key = 1;
        appAccessTokens = new HashMap();
        while (listIterator.hasNext()) {
            appAccessTokens.put(key++, listIterator.next().toString().trim());
        }
        noOfTokens = appAccessTokens.size();
    }

    @Override
    public String signURL(String url) throws Exception {
        int tokenKey = (int) (Math.random()*noOfTokens + 1);
        return new URIBuilder(url)
                .addParameter(ACCESS_TOKEN_KEY, appAccessTokens.get(tokenKey))
                .build()
                .toString();
    }
}
