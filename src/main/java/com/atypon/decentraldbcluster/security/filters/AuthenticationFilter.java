package com.atypon.decentraldbcluster.security.filters;

import com.atypon.decentraldbcluster.security.urlpattern.ExcludedUrlPattern;
import com.atypon.decentraldbcluster.security.services.JwtService;
import com.auth0.jwt.exceptions.JWTVerificationException;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Optional;

@Component
@Order(1)
public class AuthenticationFilter implements Filter, ExcludedUrlPattern {

    private final JwtService jwtService;

    @Autowired
    public AuthenticationFilter(JwtService jwtService) {
        this.jwtService = jwtService;
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain) throws ServletException, IOException {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        // For broadcast endpoint we have different authentication mechanism, handled by BootstrapFilter
        if (isExcludedUrlPattern(request.getRequestURI())) {
            chain.doFilter(request, servletResponse);
            return;
        }

        try {
            Optional<String> token = extractToken(request);

            if (token.isEmpty() || !isValidToken(token.get())) {
                response.setStatus(HttpStatus.UNAUTHORIZED.value());
                return;
            }

            request.setAttribute("token", token.get());
            chain.doFilter(servletRequest, servletResponse);

        } catch (JWTVerificationException | IOException | ServletException e) {
            response.setStatus(HttpStatus.UNAUTHORIZED.value());
        }
    }


    @Override
    public boolean isExcludedUrlPattern(String requestUri) {
        return requestUri.startsWith("/internal/api/broadcast/");
    }



    private Optional<String> extractToken(HttpServletRequest request) {
        String authorizationHeader = request.getHeader("Authorization");
        if (authorizationHeader != null && authorizationHeader.startsWith("Bearer ")) {
            return Optional.of(authorizationHeader.substring("Bearer ".length()));
        }
        return Optional.empty();
    }

    private boolean isValidToken(String token) {
        String userId = jwtService.getUserId(token);
        return userId != null;
    }

}
