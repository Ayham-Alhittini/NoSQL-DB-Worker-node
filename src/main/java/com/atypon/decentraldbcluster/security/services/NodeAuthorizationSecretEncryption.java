package com.atypon.decentraldbcluster.security.services;

import org.springframework.stereotype.Service;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;

@Service
public class NodeAuthorizationSecretEncryption {

    private final String NODE_AUTHENTICATION_KEY = "4sLUBb8wsUTkWx6eof7WdFz9Phf22joOlzYJ_IbWgqq";

    private final byte[] KEY = Base64.getUrlDecoder().decode(NODE_AUTHENTICATION_KEY);
    private final String ALGORITHM = "AES";


    public String getNodeAuthenticationKey() {
        try {
            String dataToEncrypt = System.currentTimeMillis()+"";
            SecretKeySpec keySpec = new SecretKeySpec(KEY, ALGORITHM);
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, keySpec);
            byte[] encryptedBytes = cipher.doFinal(dataToEncrypt.getBytes());
            // Encode the bytes to a Base64 URL-safe string
            return Base64.getUrlEncoder().withoutPadding().encodeToString(encryptedBytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public boolean isValidNodeAuthorizationSecret(String encryptedData) {
        try {
            byte[] decodedBytes = Base64.getUrlDecoder().decode(encryptedData);
            SecretKeySpec keySpec = new SecretKeySpec(KEY, ALGORITHM);
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, keySpec);
            byte[] decryptedBytes = cipher.doFinal(decodedBytes);
            String decryptedString = new String(decryptedBytes);
            long timestamp = Long.parseLong(decryptedString);
            long currentTimestamp = System.currentTimeMillis();
            // To check if the timestamp is within an acceptable range
            return Math.abs(currentTimestamp - timestamp) < 300000; // 300,000 ms = 5 minutes
        } catch (Exception e) {
            return false;
        }
    }

}
