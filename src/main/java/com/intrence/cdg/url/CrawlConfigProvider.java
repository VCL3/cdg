package com.intrence.cdg.url;

import com.intrence.cdg.net.FetchRequest;
import com.intrence.cdg.crawler.CrawlConfig;
import com.intrence.models.model.SearchParams;

import java.util.HashSet;
import java.util.Set;

public class CrawlConfigProvider extends ApiSeedUrlProvider {

    public CrawlConfigProvider(CrawlConfig crawlerConfig) {
        super(crawlerConfig);
    }

    @Override
    public Set<FetchRequest> buildSeedUrls(SearchParams searchParams) throws Exception {
        //Currently search params are not being used
        Set<FetchRequest> seeds = new HashSet<>();
        for (String seed : seedUrlPatterns) {
            FetchRequest req = new FetchRequest(seed, 1);
            seeds.add(req);
        }
        return seeds;
    }

    public CrawlConfig getCrawlConfig(){
        return crawlerConfig;
    }
}
