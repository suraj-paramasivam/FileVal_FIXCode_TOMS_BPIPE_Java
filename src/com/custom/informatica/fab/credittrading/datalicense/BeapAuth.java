/*
 * Copyright (c) 2019 Bloomberg Finance L.P.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.custom.informatica.fab.credittrading.datalicense;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static io.jsonwebtoken.SignatureAlgorithm.HS256;

/**
 * Class used to store credential parameters.
 */
class Credential {
    private final String clientId;
    private final byte[] clientSecret;

    /**
     * @param clientId BEAP client id used to authorize JWT tokens
     * @param clientSecret BEAP client secret used to sign JWT tokens
     */
    Credential(String clientId, byte[] clientSecret) {
        this.clientId = clientId;
        this.clientSecret = clientSecret;
    }

    /**
     * @return BEAP client id used to authorize JWT tokens
     */
    String getClientId() {
        return clientId;
    }

    /**
     * @return BEAP client secret used to sign JWT tokens
     */
    byte[] getClientSecret() {
        return clientSecret;
    }
}

class CredentialLoader {
    /**
     * Convert hex to bytes.
     * @param hexString input hexadecimal representation of byte sequence
     * @return parsed byte sequence in bytes
     */
    private static byte[] hexToBytes(String hexString) {
        if (hexString.length() % 2 == 1) {
            throw new IllegalArgumentException("Improper hex input sequence: " + hexString);
        }

        byte[] bytes = new byte[hexString.length() / 2];
        for (int i = 0; i < hexString.length(); i += 2) {
            String byte_hex = hexString.substring(i, i + 2);
            try {
                int byte_value = Integer.parseInt(byte_hex, 16);
                bytes[i / 2] = (byte) byte_value;
            } catch (NumberFormatException error) {
                throw new IllegalArgumentException("Bad hex byte value " + byte_hex);
            }
        }
        return bytes;
    }

    /**
     * Load credential file.
     * @return credentials needed to authorize into BEAP (i.e. for creating JWT tokens)
     * @throws IOException if any typical file read error occurs
     */
    static Credential loadContent(String fileName) throws IOException {
        String content = new String(Files.readAllBytes(Paths.get(fileName+".decrypted.txt")));
        JSONObject credentials = new JSONObject(content);
        String clientId = credentials.getString("client_id");
        byte[] clientSecret = hexToBytes(credentials.getString("client_secret"));

        System.out.println("Client id: " + clientId);
        String encodedSecret = Base64.getEncoder().encodeToString(clientSecret);
        System.out.println("Client secret: " + encodedSecret);

        return new Credential(clientId, clientSecret);
    }
}

/**
 * This class generates JWT tokens for outgoing HTTP BEAP requests.
 */
class JWTTokenGenerator {
    private static final int EXPIRATION_TIMEOUT = 25;
    private static final long MILLISECONDS_PER_SECONDS = 1000L;
    private static final String JWT_METHOD = "method";
    private static final String JWT_PATH = "path";
    private static final String JWT_HOST = "host";
    private static final String JWT_CLIENT_ID = "client_id";
    private static final String JWT_REGION = "region";

    private final Credential clientCredentials;

    /**
     * @param clientCredentials BEAP credentials needed to create valid JWT token.
     */
    JWTTokenGenerator(Credential clientCredentials) {
        this.clientCredentials = clientCredentials;
    }

    /**
     * Create JWT token for the specified request.
     * @param requestUrl HTTP request URL to create token for
     * @param method HTTP request method to create token for
     * @return serialized (URL-safe encoded) JWT token (with signature and header).
     */
    String createToken(URL requestUrl, String method) {
        long currentTime = System.currentTimeMillis() / MILLISECONDS_PER_SECONDS;

        final Map<String, Object> claims = new HashMap<String, Object>() {{
            put(Claims.EXPIRATION, currentTime + EXPIRATION_TIMEOUT);
            put(Claims.NOT_BEFORE, currentTime);
            put(Claims.ISSUED_AT, currentTime);
            put(Claims.ISSUER, clientCredentials.getClientId());
            put(Claims.ID, UUID.randomUUID().toString());
            put(JWT_METHOD, method);
            put(JWT_PATH, requestUrl.getPath());
            put(JWT_HOST, requestUrl.getHost());
            put(JWT_CLIENT_ID, clientCredentials.getClientId());
            put(JWT_REGION, "default");
        }};

        JwtBuilder token = Jwts.builder().setClaims(claims);
        return token.signWith(HS256, this.clientCredentials.getClientSecret()).compact();
    }
}
