/**
 * Created by wliu on 11/9/17.
 */
package com.intrence.cdg.url;

import com.intrence.cdg.net.FetchRequest;
import com.intrence.models.model.SearchParams;

import java.util.HashSet;
import java.util.Set;

public class FarfetchSeedUrlProvider implements SeedUrlProvider {

    private static final String CITIES_CONFIG_JSON_PATH = "helper-files/yelp_us_top_cities.json";
    private static final String FILTERED_CITY_URL = "https://www.yelp.com/search?find_loc=%s";
    private String source;

    public FarfetchSeedUrlProvider(String source) {
        this.source = source;
    }

    @Override
    public Set<FetchRequest> buildSeedUrls(final SearchParams searchParams) throws Exception {
        Set<FetchRequest> seedUrls = new HashSet<>();
        String url = "https://www.farfetch.com/shopping/men/dolce-gabbana-bee-print-shirt-item-11884754.aspx?storeid=9763&from=listing&tglmdl=1";

        FetchRequest fetchRequest = new FetchRequest(url, 1);

        seedUrls.add(fetchRequest);

        return seedUrls;
    }

}
