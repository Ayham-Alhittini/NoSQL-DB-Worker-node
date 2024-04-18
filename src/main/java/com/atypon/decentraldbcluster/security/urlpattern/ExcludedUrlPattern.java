package com.atypon.decentraldbcluster.security.urlpattern;

public interface ExcludedUrlPattern {
    boolean isExcludedUrlPattern(String requestUri);
}
