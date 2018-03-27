package com.intrence.cdg.crawler;

import java.util.Set;

public class CrawlablePage {
    
    private final String pageType;
    private final Set<String> urlPattern;
    private final int priority;
    
    
    public CrawlablePage(String pageType, Set<String> urlPattern, int priority) {
        this.pageType = pageType;
        this.urlPattern = urlPattern;
        this.priority = priority;
    }


    public String getPageType() {
        return pageType;
    }


    public Set<String> getUrlPatterns() {
        return urlPattern;
    }


    public int getPriority() {
        return priority;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CrawlablePage that = (CrawlablePage) o;

        if (pageType != null ? !pageType.equals(that.pageType) : that.pageType != null) return false;
        if (urlPattern != null ? !urlPattern.equals(that.urlPattern) : that.urlPattern != null) return false;
        if (priority != that.priority) return false;
        
        return true;
    }

    @Override
    public int hashCode() {
        int result = pageType != null ? pageType.hashCode() : 0;
        result = 31 * result + urlPattern.hashCode();
        result = 31 * result + priority;        
        return result;
    }
}
