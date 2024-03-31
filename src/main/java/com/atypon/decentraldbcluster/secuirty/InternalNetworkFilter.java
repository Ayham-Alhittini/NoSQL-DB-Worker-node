package com.atypon.decentraldbcluster.secuirty;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;

@Component
@Order(2)
public class InternalNetworkFilter implements Filter {

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        StringBuilder requestURL = new StringBuilder(httpRequest.getRequestURL().toString());
        Enumeration<String> parameterNames = httpRequest.getParameterNames();

        // Check if there are parameters to append
        if (parameterNames.hasMoreElements()) {
            requestURL.append("?"); // Start the query string if there are parameters
        }

        while (parameterNames.hasMoreElements()) {
            String paramName = parameterNames.nextElement();
            String[] paramValues = httpRequest.getParameterValues(paramName);

            for (int i = 0; i < paramValues.length; i++) {
                requestURL.append(URLEncoder.encode(paramName, StandardCharsets.UTF_8))
                        .append("=")
                        .append(URLEncoder.encode(paramValues[i], StandardCharsets.UTF_8));
                if (i < paramValues.length - 1) {
                    requestURL.append("&");
                }
            }

            if (parameterNames.hasMoreElements()) {
                requestURL.append("&");
            }
        }

        System.out.println("Full URL with Parameters: " + requestURL); // Log the constructed URL

        // Proceed with the filter chain
        chain.doFilter(request, response);
    }


}
