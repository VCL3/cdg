package com.intrence.cdg.url;

import com.intrence.cdg.net.FetchRequest;
import com.intrence.cdg.crawler.CrawlConfig;
import com.intrence.cdg.util.FormatUtils;
import com.intrence.models.model.SearchParams;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.HttpPost;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ApiSeedUrlProvider implements SeedUrlProvider {

    public static final int PRIORITY = 1;
    CrawlConfig crawlerConfig;
    Set<String> seedUrlPatterns = new HashSet<>();

    public ApiSeedUrlProvider(CrawlConfig crawlerConfig) {
        this.crawlerConfig = crawlerConfig;
        Set<String> seedUrls = this.crawlerConfig.getSeedUrls();
        if (seedUrls != null && seedUrls.size() > 0) {
            for (String seedUrl : seedUrls) {
                if (!StringUtils.isBlank(seedUrl)) {
                    try {
                        URL url = new URL(seedUrl);
                        seedUrlPatterns.add(seedUrl);
                    }
                    catch (MalformedURLException ex) {
                        throw new IllegalArgumentException("provided seed url is not a valid url.");
                    }
                }
            }
        }

        if (CrawlConfig.isOfflineType(crawlerConfig.getType().name())) {
            return;
        }

        // Check if this is a batch Request
        if (crawlerConfig.isBatchrequest()) {
            return;
        }

        if (seedUrlPatterns.isEmpty()) {
            throw new IllegalArgumentException("Cannot initialize ApiSeedUrlProvider, seedUrl list is empty!");
        }
    }

    @Override
    public Set<FetchRequest> buildSeedUrls(SearchParams searchParams) throws Exception {
        Set<FetchRequest> seeds = new HashSet<>();

        // if searchParams is null, there could be default seed urls that does not require searchParams,
        //  - check and return those seeds
        if (searchParams == null) {
            for (String seed : seedUrlPatterns) {
                Set<Param> paramsInSeed = paramsInSeed(seed);
                if (paramsInSeed.isEmpty()) {
                    seeds.add(new FetchRequest(seed, PRIORITY));
                }
            }
            return seeds;
        }
        searchParams = formatParams(searchParams);
        boolean postRequest = isPostRequest();
        for (String seed : seedUrlPatterns) {
            Set<Param> paramsInSearch = paramsInSearch(searchParams);
            Set<Param> paramsInSeed = paramsInSeed(seed);
            if (postRequest) {
                String body = formatRequestParams(paramsInSearch, crawlerConfig.getBody(), searchParams);
                FetchRequest req = new FetchRequest(seed, PRIORITY, HttpPost.METHOD_NAME, body);
                seeds.add(req);
            }
            else if (paramsInSeed.equals(paramsInSearch)) {
                String seedUrl = formatRequestParams(paramsInSearch, seed, searchParams);
                FetchRequest req = new FetchRequest(seedUrl, PRIORITY);
                seeds.add(req);
            }
        }

        if (seeds.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                    "Could not find any configured search urls that supports the given searchParams, searchParams=%s",
                    searchParams.toJson()));
        }

        return seeds;
    }

    private String formatRequestParams(Set<Param> paramsInSearch, String input, SearchParams searchParams) {
        if (StringUtils.isBlank(input)) {
            return input;
        }
        for (Param p : paramsInSearch) {
            input = p.formatSeed(input, searchParams);
        }
        return input;
    }

    private boolean isPostRequest() {
        String methodType = crawlerConfig.getMethodType();
        if (StringUtils.isNotBlank(methodType) && methodType.equalsIgnoreCase(HttpPost.METHOD_NAME)) {
            return true;
        }
        return false;
    }

    private Set<Param> paramsInSearch(SearchParams searchParams) {
        Set<Param> params = new HashSet<>();
        for (Param p : Param.values()) {
            if (p.getValues(searchParams).length > 0) {
                params.add(p);
            }
        }
        return params;
    }

    private Set<Param> paramsInSeed(String seed) {
        Set<Param> params = new HashSet<>();
        for (Param param : Param.values()) {
            if (seed.contains(param.pattern)) {
                params.add(param);
            }
        }

        return params;
    }

    private SearchParams formatParams(SearchParams searchParams) {
        //get the format for params from rule file
        Map<String, String> paramsFormatMap = crawlerConfig.getParamsFormat();
        if (MapUtils.isEmpty(paramsFormatMap)) {
            return searchParams;
        }
        //we support only date formatting now
        String formattedStartDate = FormatUtils.formatDates(searchParams.getStartDate(),
                                                            paramsFormatMap.get(SearchParams.START_DATE));
        String formattedEndDate = FormatUtils.formatDates(searchParams.getEndDate(),
                                                          paramsFormatMap.get(SearchParams.END_DATE));
        SearchParams formattedSearchParams = new SearchParams.Builder(searchParams)
                .startDate(formattedStartDate)
                .endDate(formattedEndDate)
                .build();
        return formattedSearchParams;
    }

    private static enum Param {

        CATEGORY("%category%", SearchParams.CATEGORY) {
            @Override
            String[] getValues(SearchParams searchParams) {
                Set<String> categories = searchParams.getCategory();
                if (CollectionUtils.isEmpty(categories)) {
                    return new String[]{};
                }

                int i = 0;
                String[] values = new String[categories.size()];
                for (String category : categories) {
                    values[i] = category;
                    i++;
                }
                return values;
            }
        },

        LOCALITY("%locality%", SearchParams.LOCALITY) {
            @Override
            String[] getValues(SearchParams searchParams) {
                return extractAsArray(searchParams.getLocality());
            }
        },

        COUNTRY("%country%", SearchParams.COUNTRY) {
            @Override
            String[] getValues(SearchParams searchParams) {
                return extractAsArray(searchParams.getCountry());
            }
        },

        REGION("%region%", SearchParams.REGION) {
            @Override
            String[] getValues(SearchParams searchParams) {
                return extractAsArray(searchParams.getRegion());
            }
        },

        POSTCODE("%postcode%", SearchParams.POSTCODE) {
            @Override
            String[] getValues(SearchParams searchParams) {
                return extractAsArray(searchParams.getPostcode());
            }
        },

        NAME("%name%", SearchParams.NAME) {
            @Override
            String[] getValues(SearchParams searchParams) {
                return extractAsArray(searchParams.getName());
            }
        },

        START_DATE("%start_date%", SearchParams.START_DATE) {
            @Override
            String[] getValues(SearchParams searchParams) {
                return extractAsArray(searchParams.getStartDate());
            }
        },

        END_DATE("%end_date%", SearchParams.END_DATE) {
            @Override
            String[] getValues(SearchParams searchParams) {
                return extractAsArray(searchParams.getEndDate());
            }
        },

        MONTH("%month%", SearchParams.MONTH) {
            @Override
            String[] getValues(SearchParams searchParams) {
                return extractAsArray(searchParams.getMonth());
            }
        },

        YEAR("%year%", SearchParams.YEAR) {
            @Override
            String[] getValues(SearchParams searchParams) {
                return extractAsArray(searchParams.getYear());
            }
        },

        QUERY("%query%", SearchParams.QUERY) {
            @Override
            String[] getValues(SearchParams searchParams) {
                return extractAsArray(searchParams.getQuery());
            }
        },

        SOURCES_TO_HUNT("%sources_to_hunt%", SearchParams.SOURCES_TO_HUNT) {
            @Override
            String[] getValues(SearchParams searchParams) {
                if (MapUtils.isEmpty(searchParams.getSourcesToHunt())) { return new String[]{}; }
                Set<String> sources = searchParams.getSourcesToHunt().keySet();
                String[] values = sources.toArray(new String[sources.size()]);
                return values;
            }

        };

        String pattern;
        String searchParamKey;

        abstract String[] getValues(SearchParams searchParams);

        String[] extractAsArray(String value) {
            if (StringUtils.isBlank(value)) {
                return new String[]{};
            }
            return new String[]{value};
        }

        String formatSeed(String seed, SearchParams searchParams) {
            int matches = StringUtils.countMatches(seed, this.pattern);
            String[] values = getValues(searchParams);
            if (matches < values.length) {
                throw new IllegalArgumentException(
                        String.format(
                                "The source does not allow searching more than %s values of %s param, seed=%s, searchParams=%s",
                                matches,
                                this.searchParamKey,
                                seed,
                                Arrays.asList(values)));
            }

            // if we have more %values% allowed in seedurl than the number of values in search-param,
            //  then search-params will repeat

            // for example if seed-url config is ?category="%category%","%category%,"%category%", and if search-param is coffee,tea 
            //  , then formatted seed url would look like ?category="coffee","tea","coffee"
            int j = 0;
            for (int i = 0; i < matches; i++) {
                if (j == values.length) {
                    j = 0;
                }
                seed = StringUtils.replaceOnce(seed, this.pattern, values[j]);
                j++;
            }
            return seed;
        }

        Param(String pattern, String searchParamKey) {
            this.pattern = pattern;
            this.searchParamKey = searchParamKey;
        }
    }
}
