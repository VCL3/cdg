package com.intrence.cdg.net.header;

import com.google.common.collect.ImmutableMap;
import com.intrence.config.collection.ConfigMap;
import com.intrence.cdg.net.WebFetcher;
import com.intrence.cdg.util.Constants;

import java.util.Map;

public class HeaderFactory {
    private static Map<String, HeaderCreator> headerCreatorInstances;

    public static HeaderCreator getHeaderCreator(ConfigMap configMap, String source, WebFetcher webFetcher) {
        if (headerCreatorInstances == null) {
            initializeHeaderCreators(configMap, webFetcher);
        }
        return headerCreatorInstances.get(source);
    }

    private static void initializeHeaderCreators(ConfigMap configMap, WebFetcher webFetcher) {
        headerCreatorInstances = new ImmutableMap.Builder<String, HeaderCreator>()
//                .put(Constants.FARFETCH_ID, new FarfetchHeaderCreator(configMap, webFetcher))
                .build();
    }
}
