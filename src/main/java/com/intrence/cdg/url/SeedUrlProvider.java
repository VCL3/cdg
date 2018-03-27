
package com.intrence.cdg.url;

import com.intrence.cdg.net.FetchRequest;
import com.intrence.models.model.SearchParams;

import java.util.Set;

public interface SeedUrlProvider {    
    /**
     * Return seed URLs when there are searchParams
     * @param searchParams
     * @return
     * @throws Exception
     */
    Set<FetchRequest> buildSeedUrls(SearchParams searchParams) throws Exception;
}
