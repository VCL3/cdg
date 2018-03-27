/**
 * Created by wliu on 11/8/17.
 */
package com.intrence.cdg.parser;

import com.intrence.cdg.net.FetchRequest;

import java.util.Set;

public interface Parser<T> {
    Set<FetchRequest> extractLinks();
    Set<T> extractEntities();
    Set<String> extractEntitiesAsJson();
}
