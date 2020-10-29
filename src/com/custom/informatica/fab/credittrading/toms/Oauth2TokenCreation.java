package com.custom.informatica.fab.credittrading.toms;

import com.auth0.jwt.JWT;
import com.bloomberg.wag.sdk.RequestClient;
import com.bloomberg.wag.sdk.Utils;
import com.bloomberg.wag.sdk.usermode.AuthenticationException;
import com.bloomberg.wag.sdk.usermode.DeviceModeFlow;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import java.io.*;
import java.net.HttpURLConnection;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Oauth2TokenCreation {


    /**
     * Insert your client ID
     */
    private static final String CLIENT_ID = "5484a6b19790220c285982fb7e5675d7";

    /**
     * Insert your secret
     */
    private static final String SECRET = "fbbd6baa3e1347334bfd31cf21db5f7a560f32b0c70e502f632a048e5dec05bb";

    /**
     * Insert accessing scopes
     */
    private static final List<String> SCOPES = new ArrayList<String>();

    public static Map<String,String> generatedToken = null;


    public static void main(String[] args) throws IOException, AuthenticationException, InterruptedException {
        //System.setProperty("https.proxyHost", "10.163.0.84");
        //System.setProperty("https.proxyPort", "8080");
        SCOPES.add("ts");
        final DeviceModeFlow flow = new DeviceModeFlow(
                "https://beta.api.bloomberg.com",
                60 * 1000,
                CLIENT_ID,
                SECRET,
                SCOPES
        );

        // Get url for user to login
        final String loginUrl = flow.authenticate();
        System.out.println(loginUrl);
        FileWriter f = new FileWriter("AuthURL.txt");
        BufferedWriter bw = new BufferedWriter(f);
        PrintWriter pw = new PrintWriter(bw);
        pw.write(loginUrl);
        pw.flush();
        pw.close();
        bw.close();
        System.out.println("Hit enter after log-in...");
        //noinspection ResultOfMethodCallIgnored
        System.in.read();
        //Thread.sleep(90000);


        // Fetch required tokens after logged in
        flow.fetchTokens();
        generatedToken = flow.getTokens();
        writeToFile(generatedToken);
        // Get the RequestClient with user information
        RequestClient client = flow.getRequestClient();

        writeObjtoFile(flow);

        // GET request with query parameters
        HttpURLConnection response = client.getRequestBuilder("GET", "/sandbox/v1/user-mode/ping")
                .doRequest();
        print(response);

        // Refresh tokens if they're expired. If refreshToken is also expire, user need to login again
        flow.refreshTokens();
        generatedToken = flow.getTokens();


        // Need to get a new client after any token update
        client = flow.getRequestClient();

        // Request with custom JWT claim
        response = client.getRequestBuilder("GET", "/sandbox/v1/user-mode/ping")
                .withCustomJwtBuilder(JWT.create()
                        .withClaim("name", "value"))
                .doRequest();

        print(response);
    }

    public static void writeToFile(Map<String, String> map) throws IOException {

        File f = new File("OAT_TOMS");
        FileOutputStream fos = new FileOutputStream(f);
        PrintWriter pw = new PrintWriter(fos);
        for (Map.Entry<String, String> entry : map.entrySet()){
            pw.println(entry.getKey()+"="+entry.getValue());
        }

        pw.flush();
        pw.close();
        fos.close();
    }

    public static void writeObjtoFile(Object o) throws IOException {

 Gson gson=new Gson();
 String json = gson.toJson(o);
 System.out.println(json);
 FileWriter fw  = new FileWriter("OAT_TOMS_OBJECT");
 fw.write(json);
 fw.close();
    }

    private static void print(HttpURLConnection response) throws IOException {
        System.out.println(response.getRequestMethod() + " " + response.getURL());

        System.out.println("Response:");

        System.out.println(response.getHeaderFields());

        try (Scanner scanner = new Scanner(response.getInputStream())) {
            while (scanner.hasNextLine()) {
                System.out.println(scanner.nextLine());
            }
        } catch (Exception e) {
            Scanner scanner = new Scanner(response.getErrorStream());
            while (scanner.hasNextLine()) {
                System.out.println(scanner.nextLine());
            }
        }

        System.out.println();
    }
}
