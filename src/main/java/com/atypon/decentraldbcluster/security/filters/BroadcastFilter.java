package com.atypon.decentraldbcluster.security.filters;

import com.atypon.decentraldbcluster.security.services.NodeAuthorizationSecretEncryption;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Order(1)
@Component
public class BroadcastFilter implements Filter {

    private final NodeAuthorizationSecretEncryption nodeAuthorization;

    @Autowired
    public BroadcastFilter(NodeAuthorizationSecretEncryption nodeAuthorizationSecretEncryption) {
        this.nodeAuthorization = nodeAuthorizationSecretEncryption;
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain) throws IOException, ServletException {

        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        String nodeAuthorization = request.getHeader("X-Node-Authorization");

        if (nodeAuthorization == null) {
            response.setStatus(403); // It's forbidden not unauthorized because authorization is passed
            return;
        }

        if (!this.nodeAuthorization.isValidNodeAuthorizationSecret(nodeAuthorization)) {
            response.setStatus(403); // It's forbidden not unauthorized because authorization is passed
            return;
        }

        chain.doFilter(request, response);
    }
}


