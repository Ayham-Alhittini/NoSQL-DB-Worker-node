package com.atypon.decentraldbcluster.test.filter;

import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FilterConfig {
    @Bean
    public FilterRegistrationBean<NodeAffinityFilter> nodeAffinityFilterRegistration(NodeAffinityFilter nodeAffinityFilter) {
        FilterRegistrationBean<NodeAffinityFilter> registrationBean = new FilterRegistrationBean<>();
        registrationBean.setFilter(nodeAffinityFilter);
        registrationBean.addUrlPatterns(
                "/api/document/deleteDocument/*",
                "/api/document/updateDocument/*",
                "/api/document/replaceDocument/*"
        );
        return registrationBean;
    }
}
