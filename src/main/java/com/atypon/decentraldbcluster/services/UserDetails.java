package com.atypon.decentraldbcluster.services;

import com.atypon.decentraldbcluster.secuirty.JwtService;
import com.auth0.jwt.exceptions.JWTVerificationException;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class UserDetails {
    private final JwtService jwtService;

    @Autowired
    public UserDetails(JwtService jwtService) {
        this.jwtService = jwtService;
    }

    public String getUserDirectory(HttpServletRequest request) {
        return getUserId(request);
    }

    public String getUserId(HttpServletRequest request) throws JWTVerificationException {
        String token = (String) request.getAttribute("token");
        return jwtService.getUserId(token);
    }

    public String getUsername(HttpServletRequest request) throws JWTVerificationException {
        String token = (String) request.getAttribute("token");
        return jwtService.getUsernameFromToken(token);
    }

    public String getEmail(HttpServletRequest request) throws JWTVerificationException {
        String token = (String) request.getAttribute("token");
        return jwtService.getEmailFromToken(token);
    }
}
