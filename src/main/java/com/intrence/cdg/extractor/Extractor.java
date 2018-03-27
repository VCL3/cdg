/**
 * Created by wliu on 11/8/17.
 */
package com.intrence.cdg.extractor;

import com.fasterxml.jackson.databind.JsonNode;
import com.intrence.cdg.net.FetchRequest;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public interface Extractor<N> {

    /*
     * Get parsed content tree.
     * @return contentTree
    */
    N getContentTree();

    /**
     * This method returns the respective content presented in string to TagNode or JsonNode
     *
     * @param content string of a source page
     * @return content tree either in TagNode or JsonNode, etc
     */
    N getContentTree(String content);

    /*
     * Get contentTree in string format.
     * @return contentTreeString
    */
    String getContentString();

    /*
     * Takes extract entity function as parameter and spawn the function to extract entities.
     * @param function to extract entity.
     * @return Set of extracted entities from contentTree
    */
    <T> Set<T> extractEntities(Function<N, T> extractEntityFunction);

    /*
     * Extract outLinks from contentTree.
     * @param pageType.
     * @param extractionPattern
     * @return Set of extracted links from contentTree
    */
    Set<FetchRequest> extractLinks(String pageType, String extractionPattern);

    /*
     * Extract field from contentTree
     * @param extractionRule.
     * @param contentNode
     * @return extractedField
    */
    String extractField(String rule, N contentTree);

    /*
     * Extract list of fields from contentTree
     * @param extractionRule.
     * @param contentNode
     * @return List of extractedFields
    */
    List<String> extractFields(String rule, N contentTree);

    /*
     * Extract field object from contentTree
     * @param extractionRule.
     * @param contentNode
     * @return extractedFieldObject
    */
    N extractFieldObject(String rule, N contentTree);

    /*
     * Extract array of fields from contentTree
     * @param extractionRule.
     * @param contentNode
     * @return Array of extractedFields
    */
    N[] extractFieldObjects(String rule, N contentTree);

    /*
     * Extract and concatenate fields from contentTree
     * @param extractionRule.
     * @param contentNode
     * @return extractedConcatenateFields
    */
    String extractAndConcatenateFields(String rule, N contentTree);

    /*
     * Extract repeated fields from contentTree
     * @param extractionRules.
     * @param contentNode
     * @return List of repeated extracted fields
    */
    List<Map<String,String>> extractRepeatedFields(JsonNode repeatedRules, N contentTree);

    String getNodeString(N node);

}
