package com.intrence.cdg.net.signature;

import com.google.common.collect.ImmutableMap;
import com.intrence.config.collection.ConfigMap;
import com.intrence.cdg.util.Constants;

import java.util.Map;

public class SignatureFactory {

    private static Map<String, SignCreator> signCreatorInstances;

    public static SignCreator getSignCreator(ConfigMap configMap, String source) {
        if (signCreatorInstances == null) {
            lazilyInitializeSignCreators(configMap);
        }
        return signCreatorInstances.get(source);
    }

    private static void lazilyInitializeSignCreators(ConfigMap configMap) {
        signCreatorInstances = new ImmutableMap.Builder<String, SignCreator>()
//                .put(Constants.FARFETCH_ID, new FarfetchSignCreator(configMap))
                .build();
    }
}
