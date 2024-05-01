package com.atypon.decentraldbcluster.security.urlpattern;

import com.atypon.decentraldbcluster.security.filters.AdminFilter;
import com.atypon.decentraldbcluster.security.filters.BroadcastFilter;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SecurityFiltersRegister {

    @Bean
    public FilterRegistrationBean<BroadcastFilter> BroadcastFilterRegistration(BroadcastFilter broadcastFilter) {
        FilterRegistrationBean<BroadcastFilter> registrationBean = new FilterRegistrationBean<>();
        registrationBean.setFilter(broadcastFilter);
        registrationBean.addUrlPatterns(
            "/internal/api/broadcast/*"
        );
        return registrationBean;
    }

    @Bean
    public FilterRegistrationBean<AdminFilter> BootstrapFilterRegistration(AdminFilter bootstrapFilter) {
        FilterRegistrationBean<AdminFilter> registrationBean = new FilterRegistrationBean<>();
        registrationBean.setFilter(bootstrapFilter);
        registrationBean.addUrlPatterns(
            "/internal/api/bootstrap/*",
            "/api/backup/*"
        );
        return registrationBean;
    }

}
