package com.atypon.decentraldbcluster.security.filters;

import com.atypon.decentraldbcluster.communication.config.NodeCommunicationConfiguration;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.annotation.Order;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;

@Component
@Order(2)
public class BootstrapFilter implements Filter {

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain) throws IOException, ServletException {

        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        RestTemplate restTemplate = new RestTemplate();

        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", request.getHeader("Authorization"));
        HttpEntity<Object> requestEntity = new HttpEntity<>(null, headers);

        String requestUrl = NodeCommunicationConfiguration.getNodeAddress(8080) + "/api/auth/isAdminUser";

        try {
            restTemplate.exchange( requestUrl , HttpMethod.GET, requestEntity, Void.class);
            chain.doFilter(request, response);
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            response.setContentType("application/json"); // Set the content type to JSON or text/plain etc.
            String responseBody = "{\"error\": \"" + e.getMessage() + "\"}"; // JSON formatted error message
            response.getWriter().write(responseBody); // Write the response body
        }
    }
}
