package com.atypon.decentraldbcluster.affinity;

import com.atypon.decentraldbcluster.config.NodeConfiguration;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;

public class RedirectToAffinity {
    public static Object redirect(HttpServletRequest request, JsonNode body, HttpMethod method, int affinityPort) {
        RestTemplate restTemplate = new RestTemplate();

        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", request.getHeader("Authorization"));
        HttpEntity<JsonNode> requestEntity = new HttpEntity<>(body, headers);

        return restTemplate.exchange( getRequestUrl(request, affinityPort) , method, requestEntity, Object.class);
    }

    private static String getRequestUrl(HttpServletRequest request, int affinityPort) {
        StringBuilder requestURL = new StringBuilder(request.getRequestURL().toString());
        Enumeration<String> parameterNames = request.getParameterNames();

        // Check if there are parameters to append
        if (parameterNames.hasMoreElements()) {
            requestURL.append("?"); // Start the query string if there are parameters
        }

        while (parameterNames.hasMoreElements()) {
            String paramName = parameterNames.nextElement();
            String[] paramValues = request.getParameterValues(paramName);

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

        return requestURL.toString().replace("http://localhost:" +  NodeConfiguration.getCurrentNodePort(), NodeConfiguration.getNodeAddress(affinityPort));
    }
}
