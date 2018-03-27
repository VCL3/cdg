package com.intrence.cdg.frontier;

import com.intrence.cdg.net.FetchRequest;

import java.util.Set;

public interface FetchRequestFrontier {

    FetchRequest getNext(int taskRuleId);
    
    Set<FetchRequest> getTopK(int taskRuleId, int K);
    
    void add(int taskRuleId, FetchRequest fetchReq);
    
    void addAll(int taskRuleId, Set<FetchRequest> fetchReqs);
    
    void delete(int taskRuleId, FetchRequest fetchReq);
    
    /**
     * Takes a set<> of fetchReqs as argument and deletes them if they are found associated with the key taskRuleId
     * @param taskRuleId
     * @param fetchReqs
     */
    void deleteAll(int taskRuleId, Set<FetchRequest> fetchReqs);
    
    /**
     * deletes the taskRuleId key and all its associated values
     * @param taskRuleId
     */
    void deleteKey(int taskRuleId);
}
