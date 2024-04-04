package com.atypon.decentraldbcluster.affinity;

import com.atypon.decentraldbcluster.config.NodeConfiguration;
import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.error.ResourceNotFoundException;
import com.atypon.decentraldbcluster.query.QueryExecutor;
import com.atypon.decentraldbcluster.query.base.Query;
import com.atypon.decentraldbcluster.query.documents.DocumentQueryBuilder;
import com.atypon.decentraldbcluster.services.UserDetails;
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

    private final UserDetails userDetails;
    private final QueryExecutor queryExecutor;

    @Autowired
    public NodeAffinityFilter(UserDetails userDetails, QueryExecutor queryExecutor) {
        this.userDetails = userDetails;
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
            Query query = getQueryFromBuilder(userDetails.getUserId(request), requestParts);
            Document document = queryExecutor.exec(query, Document.class);
            if (!isAssignedNode(document)) {
                redirectToAffinity(request, response, requestURI, document.getAffinityPort());
                return;
            }
            request.setAttribute("document", document);
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

    private boolean isAssignedNode(Document document) {
        return document.getAffinityPort() == NodeConfiguration.getCurrentNodePort();
    }

    private void redirectToAffinity(HttpServletRequest request, HttpServletResponse response, String requestURI, int affinityPort) {
        String redirectUrl = "http://localhost:" + affinityPort;
        redirectUrl += requestURI;
        redirectUrl += assignDocumentVersionParam(request);

        response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT); // 307 status code
        response.setHeader("Location", redirectUrl);
    }

    private String assignDocumentVersionParam(HttpServletRequest request) {
        String paramName = "expectedVersion";
        String paramValue = request.getParameter(paramName);

        if (paramValue != null)
            return "?" + paramName + "=" + paramValue;
        return "";
    }
}
