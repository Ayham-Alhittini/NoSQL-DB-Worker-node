package com.atypon.decentraldbcluster.secuirty;

import com.auth0.jwt.exceptions.JWTVerificationException;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Optional;

@Component
public class AuthenticationFilter implements Filter {

    private final JwtService jwtService;

    @Autowired
    public AuthenticationFilter(JwtService jwtService) {
        this.jwtService = jwtService;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        try {
            Optional<String> token = extractToken(httpRequest);

            if (token.isEmpty() || !isValidToken(token.get())) {
                httpResponse.setStatus(HttpStatus.UNAUTHORIZED.value());
                return;
            }

            httpRequest.setAttribute("token", token.get());
            chain.doFilter(request, response);

        } catch (JWTVerificationException | IOException | ServletException e) {
            httpResponse.setStatus(HttpStatus.UNAUTHORIZED.value());
        }
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
