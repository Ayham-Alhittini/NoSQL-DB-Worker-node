package com.atypon.decentraldbcluster.secuirty;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.interfaces.DecodedJWT;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class JwtUtil {
    @Value("${jwt.secret}")
    private String secret;

    public String getUserId(String token) throws JWTVerificationException {
        return getJwt(token)
                .getSubject();
    }

    public String getUsernameFromToken(String token) throws JWTVerificationException {
        return getJwt(token)
                .getClaim("username").asString();
    }

    public String getEmailFromToken(String token) throws JWTVerificationException {
        return getJwt(token)
                .getClaim("email").asString();
    }

    private DecodedJWT getJwt(String token) throws JWTVerificationException {
        Algorithm algorithm = Algorithm.HMAC256(secret);
        JWTVerifier verifier = JWT.require(algorithm).build();
        return verifier.verify(token);
    }


}