package com.atypon.decentraldbcluster.affinity;

import com.atypon.decentraldbcluster.config.NodeConfiguration;
import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.error.ResourceNotFoundException;
import com.atypon.decentraldbcluster.services.DocumentReaderService;
import com.atypon.decentraldbcluster.services.PathConstructor;
import com.atypon.decentraldbcluster.services.UserDetails;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Order(2)
public class NodeAffinityFilter implements Filter {

    private final UserDetails userDetails;
    private final DocumentReaderService documentService;

    public NodeAffinityFilter(UserDetails userDetails, DocumentReaderService documentService) {
        this.userDetails = userDetails;
        this.documentService = documentService;
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        String path = request.getRequestURI();
        String[] pathParts = path.split("/");

        //
        if (pathParts.length < 5) {
            chain.doFilter(request, response);
            return;
        }

        String database = pathParts[4];
        String collection = pathParts[5];
        String documentId = pathParts[6];

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = PathConstructor.constructCollectionPath(userDirectory, database, collection);
        String documentPath = PathConstructor.constructDocumentPath(collectionPath, documentId);

        try {
            Document document = documentService.readDocument(documentPath);
            if (!isAssignedNode(document)) {

                String redirectUrl = "http://localhost:" + document.getAffinityPort();
                redirectUrl += path;
                redirectUrl += assignDocumentVersionParam(request);

                response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT); // 307 status code
                response.setHeader("Location", redirectUrl);
                return;
            }

            request.setAttribute("document", document);
            chain.doFilter(request, response);
        } catch (ResourceNotFoundException e) {
            response.setStatus(HttpStatus.NOT_FOUND.value());
        }
    }

    private boolean isAssignedNode(Document document) {
        return document.getAffinityPort() == NodeConfiguration.getCurrentNodePort();
    }

    private String assignDocumentVersionParam(HttpServletRequest request) {
        String paramName = "expectedVersion";
        String paramValue = request.getParameter(paramName);

        if (paramValue != null)
            return "?" + paramName + "=" + paramValue;
        return "";
    }
}
