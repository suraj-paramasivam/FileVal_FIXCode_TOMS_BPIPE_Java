package com.custom.informatica.fab.credittrading.toms;

import java.io.IOException;
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

/**
 * Load Credentials Class - used to load credentials from the decrypted credentials file. Call this after the decryption call is complete
 * For both methods in this class, use decryptFileName as the parameter, please pass the entire path of the decrypted file
 */


public class LoadCredentials {




    public String loadClientID(String decryptFileName) throws IOException {
        String content = new String(Files.readAllBytes(Paths.get(decryptFileName)));
        JSONObject credentials = new JSONObject(content);
        String clientId = credentials.getString("client_id");
        return clientId;
    }

    public String loadClientSecret(String decryptFileName) throws IOException {
        String content = new String(Files.readAllBytes(Paths.get(decryptFileName)));
        JSONObject credentials = new JSONObject(content);
        String clientSecret = credentials.getString("client_secret");
        return clientSecret;
    }


}
