package com.atypon.decentraldbcluster.test.filter;

import com.atypon.decentraldbcluster.communication.config.NodeCommunicationConfiguration;
import com.atypon.decentraldbcluster.exceptions.types.ResourceNotFoundException;
import com.atypon.decentraldbcluster.query.executors.QueryExecutor;
import com.atypon.decentraldbcluster.query.types.Query;
import com.atypon.decentraldbcluster.test.builder.DocumentQueryBuilder;
import com.atypon.decentraldbcluster.security.services.JwtService;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Order(2)
public class NodeAffinityFilter implements Filter {

    private final JwtService jwtService;
    private final QueryExecutor queryExecutor;

    @Autowired
    public NodeAffinityFilter(JwtService jwtService, QueryExecutor queryExecutor) {
        this.jwtService = jwtService;
        this.queryExecutor = queryExecutor;
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        String requestURI = request.getRequestURI();
        String[] requestParts = requestURI.split("/");

        //Invalid
        if (requestParts.length < 5) {
            chain.doFilter(request, response);
            return;
        }

        try {
            Query query = getQueryFromBuilder(jwtService.getUserId(request), requestParts);
            JsonNode document = queryExecutor.exec(query, JsonNode.class);
            int documentAffinityPort = extractNodePortFromDocumentId(document.get("object_id").asText());
            if (documentAffinityPort != NodeCommunicationConfiguration.getCurrentNodePort()) {
                redirectToAffinity(response, requestURI, documentAffinityPort);
                return;
            }
            chain.doFilter(request, response);
        } catch (ResourceNotFoundException e) {
            response.setStatus(HttpStatus.NOT_FOUND.value());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Query getQueryFromBuilder(String originator, String[] requestParts) {
        return new DocumentQueryBuilder()
                .withOriginator(originator)
                .withDatabase(requestParts[4])
                .withCollection(requestParts[5])
                .selectDocuments()
                .withId(requestParts[6])
                .build();
    }

    private int extractNodePortFromDocumentId(String documentId) {
        int basePort = 8080;
        // The last digit in the ID represent the node number
        int nodeNumber = documentId.charAt(documentId.length() - 1) - '0';
        return basePort + nodeNumber;
    }

    private void redirectToAffinity(HttpServletResponse response, String requestURI, int affinityPort) {
        String redirectUrl = "http://localhost:" + affinityPort;
        redirectUrl += requestURI;

        response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT); // 307 status code
        response.setHeader("Location", redirectUrl);
    }

}