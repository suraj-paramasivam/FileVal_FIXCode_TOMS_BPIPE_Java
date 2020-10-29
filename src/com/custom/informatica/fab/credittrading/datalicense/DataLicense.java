package com.custom.informatica.fab.credittrading.datalicense;


import com.custom.informatica.fab.credittrading.common.EncryptFilewithPass;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.GeneralSecurityException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This class creates a new request using the new resource based request builder framework.
 */
public class DataLicense {
    public static final Logger logger = Logger.getLogger(DataLicense.class);
    private static final String PROTOCOL = "https";
    private static final String HOST = "api.bloomberg.com";
    private static final String CATALOGS_PATH = "/eap/catalogs/";
    private static final String NOTIFICATIONS_PATH = "/eap/notifications/sse";
    private static final String UNIVERSES_PATH_TEMPLATE = "/eap/catalogs/%s/universes/";
    private static final String FIELD_LISTS_PATH_TEMPLATE = "/eap/catalogs/%s/fieldLists/";
    private static final String TRIGGERS_PATH_TEMPLATE = "/eap/catalogs/%s/triggers/";
    private static final String REQUESTS_PATH_TEMPLATE = "/eap/catalogs/%s/requests/";
    private static final String REPLY_PATH_TEMPLATE = "/eap/catalogs/%s/datasets/%s/snapshots/%s/distributions/%s";
    private static final String DOWNLOADS_DIRECTORY = "SrcFiles";
    private static final String DOWNLOAD_FILE_TEMPLATE = "%s.gz";

    private static final DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    private static final String timeStamp = dateFormat.format(System.currentTimeMillis());

    private static final String UNIVERSE_ID = "FABCreditTradingUniverse1" + timeStamp;
    private static final String FIELD_LIST_ID = "FABCreditTradingFL1" + timeStamp;
    private static final String TRIGGER_ID = "FABCreditTradingTrigger1" + timeStamp;
    private static final String REQUEST_ID = "myReq" + timeStamp;
    private static final String REPLY_ID = REQUEST_ID + ".bbg";

    private static final String HEADER_CONTENT_TYPE = "Content-Type";
    private static final String CONTENT_TYPE_JSON = "application/json";
    private static final String HEADER_ACCEPT_ENCODING = "Accept-Encoding";
    private static final String ENCODING_GZIP = "gzip";
    private static final int MAX_REDIRECTS = 3;
    private static final int CONNECTION_TIMEOUT_MILLISECONDS = 30000;
    private static String decryptFileName=null;
    private static String password=null;

    //static {
    //    ProxyAuthentication.setUpProxyAuthentication();
    //}
    private static final int PERMANENT_REDIRECT = 308;
    private static final int TEMPORARY_REDIRECT = 307;

    /**
     * Create component that will be used to create BEAP JWT tokens.
     *
     * @return created JWT token maker
     */
    private static JWTTokenGenerator loadAuthenticator() {
        Credential clientAuthentication;
        try {
            EncryptFilewithPass.decryptFile(decryptFileName, password);
            File file = new File(decryptFileName);
            if (file.delete()) {
                logger.debug("Decrypted file deleted successfully");

            } else {
                logger.error("Something went wrong with file delete, please delete this file manually - credentials/credential_toms.txt.decrypted.txt");

            }
            clientAuthentication = CredentialLoader.loadContent(decryptFileName);
            return new JWTTokenGenerator(clientAuthentication);
        } catch (FileNotFoundException e) {
            logger.error("Credential file not found: " + e);
            System.exit(1);
        } catch (IOException e) {
            logger.error("File read error: " + e);
            System.exit(1);
        } catch (GeneralSecurityException e) {
           logger.error(e);
           System.exit(1);
        }
        return null;
    }

    /**
     * Helper method used to detect whether HTTP status code denotes a HTTP redirection.
     *
     * @param statusCode HTTP response status code
     * @return True if the status code denotes a redirection, false otherwise
     */
    private static boolean isRedirect(final int statusCode) {
        switch (statusCode) {
            case HttpURLConnection.HTTP_MOVED_PERM: // 301
            case HttpURLConnection.HTTP_MOVED_TEMP: // 302
            case HttpURLConnection.HTTP_SEE_OTHER:  // 303
            case PERMANENT_REDIRECT:
            case TEMPORARY_REDIRECT:
                return true;
            default:
                return false;
        }
    }

    /**
     * Detect whether HTTP redirection method should be changed to GET according to the HTTP standard.
     *
     * @param statusCode     HTTP status code of redirected response
     * @param originalMethod HTTP method of original response
     * @return true if the following request method should be changed to 'GET'
     */
    private static boolean forceGet(final int statusCode, String originalMethod) {
        if (originalMethod.toUpperCase().equals("POST")) {
            switch (statusCode) {
                case HttpURLConnection.HTTP_MULT_CHOICE:  // 300
                case HttpURLConnection.HTTP_MOVED_PERM:   // 301
                case HttpURLConnection.HTTP_MOVED_TEMP:   // 302
                case HttpURLConnection.HTTP_SEE_OTHER:    // 303
                    return true;
            }
        }
        return false;
    }

    /**
     * Make HTTP request to BEAP server.
     * NOTE: Handling redirection is required by BEAP documentation, also note that while handling redirection - new
     * JWT token should be generated and assigned to the each subsequent HTTP request caused by redirection.
     *
     * @param tokenMaker         JWT token maker created from client's credentials
     * @param uri                URI of the requested resource
     * @param httpHeaders        set of HTTP header parameters
     * @param redirectsRemaining maximum number of potential redirection attempts
     * @return HTTP session containing response results
     * @throws IOException in case of unsuccessful HTTP request
     */
    private static HttpURLConnection httpRequest(JWTTokenGenerator tokenMaker,
                                                 String method,
                                                 URL uri,
                                                 Map<String, String> httpHeaders,
                                                 String payload,
                                                 int redirectsRemaining) throws IOException {
        HttpURLConnection session = null;
        String token = tokenMaker.createToken(uri, method);
        try {
            session = (HttpURLConnection) uri.openConnection();
            session.setRequestMethod(method);
            session.setRequestProperty("JWT", token);
            logger.debug("Setting Request property to JWT");
            session.setRequestProperty("api-version", "2");
            logger.debug("Setting api version to 2");
            session.setConnectTimeout(CONNECTION_TIMEOUT_MILLISECONDS);
            session.setReadTimeout(CONNECTION_TIMEOUT_MILLISECONDS);
            session.setInstanceFollowRedirects(false);

            if (httpHeaders != null) {
                for (Map.Entry<String, String> property : httpHeaders.entrySet()) {
                    session.setRequestProperty(property.getKey(), property.getValue());
                }
            }

            if (payload != null) {
                session.setDoOutput(true);
                try (DataOutputStream body = new DataOutputStream(session.getOutputStream())) {
                    body.write(payload.getBytes());
                }
            }

            int status = session.getResponseCode();

            if (status == HttpURLConnection.HTTP_FORBIDDEN) {
                logger.error("Either supplied credentials are invalid or expired, or the requesting IP address is not on the allowlist.");
                System.exit(1);
            }

            if (isRedirect(status)) {
                if (redirectsRemaining == 0) {
                    logger.error("Too many redirects");
                    throw new IOException("Too many redirects.");
                }

                String redirectLocation = session.getHeaderField("Location");
                session.disconnect();
                uri = new URL(uri.getProtocol(), uri.getHost(), redirectLocation);
                if (forceGet(status, method)) {
                    return httpRequest(tokenMaker, "GET", uri, null, null, redirectsRemaining - 1);
                } else {
                    return httpRequest(tokenMaker, method, uri, httpHeaders, payload, redirectsRemaining - 1);
                }
            }
        } catch (IOException ioerror) {
            logger.error("Caught IO Error"+ioerror);
            if (session != null) {
                session.disconnect();
                logger.info("Session Disconnected");
            }
            throw ioerror;
        }
        return session;
    }

    /**
     * Send a GET request to BEAP server.
     *
     * @param tokenMaker   JWT token maker created from client's credentials
     * @param path         path to the requested resource
     * @param maxRedirects maximum number of potential redirection attempts
     * @return HTTP session containing response results
     * @throws IOException in case of unsuccessful HTTP request
     */
    private static HttpURLConnection httpGet(JWTTokenGenerator tokenMaker,
                                             String path,
                                             int maxRedirects) throws IOException {
        URL url = new URL(PROTOCOL, HOST, path);
        return httpRequest(tokenMaker, "GET", url, null, null, maxRedirects);
    }

    /**
     * Send a POST request to BEAP server.
     *
     * @param tokenMaker   JWT token maker created from client's credentials
     * @param path         path to the requested resource
     * @param contentType  media type of the requested resource
     * @param maxRedirects maximum number of potential redirection attempts
     * @return HTTP session containing response results
     * @throws IOException in case of unsuccessful HTTP request
     */
    private static HttpURLConnection httpPost(JWTTokenGenerator tokenMaker,
                                              String path,
                                              String contentType,
                                              JSONObject payload,
                                              int maxRedirects) throws IOException {
        URL url = new URL(PROTOCOL, HOST, path);
        String serializedPayload = payload.toString(2);

        logger.debug("\nPosting to %s, json content: \n"+url);
        logger.debug(serializedPayload);


        Map<String, String> httpHeaders = new HashMap<String, String>() {{
            put(HEADER_CONTENT_TYPE, contentType);
        }};
        return httpRequest(tokenMaker, "POST", url, httpHeaders, serializedPayload, maxRedirects);
    }

    /**
     * Deserialize JSON data from HTTP response.
     *
     * @param session HTTP processed session containing HTTP response
     * @return deserialized JSON instance from response HTTP body
     * @throws IOException in case if HTTP body loading and decoding was not successful
     */
    private static JSONObject getJsonContent(HttpURLConnection session) throws IOException {
        Reader responseStream;
        try {
            responseStream = new InputStreamReader(session.getInputStream(), UTF_8);
        } catch (IOException ioerror) {
            logger.error("IO Error : Unexpected Server response: "+ioerror);
            InputStream errorContent = session.getErrorStream();
            if (errorContent == null)

                throw new IOException("Unexpected server response: " + session.getResponseMessage());
            responseStream = new InputStreamReader(errorContent, UTF_8);
        }

        try (BufferedReader input = new BufferedReader(responseStream)) {
            String content = input.lines().collect(Collectors.joining(System.lineSeparator()));
            JSONObject jsonBody = new JSONObject(content);
            if (jsonBody.has("error")) {
                logger.error(jsonBody.getString("error_description"));
                System.exit(1);
            }
            return jsonBody;
        }
    }

    /**
     * Request a list of available catalogs. Retrieve the one corresponding to Data License account number (detect this
     * by "scheduled" subscription type).
     *
     * @param tokenMaker JWT token maker created from client's credentials
     * @return catalog name
     * @throws IOException in case if HTTP body loading and decoding was not successful
     */
    private static String loadAccountCatalog(JWTTokenGenerator tokenMaker) throws IOException {
        HttpURLConnection session = httpGet(tokenMaker, CATALOGS_PATH, MAX_REDIRECTS);
        try {
            JSONArray catalogs = getJsonContent(session).getJSONArray("contains");
            for (int i = 0; i < catalogs.length(); ++i) {
                JSONObject member = catalogs.getJSONObject(i);
                if (member.getString("subscriptionType").equals("scheduled")) {
                    return member.getString("identifier");
                }
            }

            throw new IOException("Cannot find scheduled catalog, probably improper credential file is used.");
        } finally {
            session.disconnect();
        }
    }

    /**
     * Create a new universe resource.
     *
     * @param tokenMaker JWT token maker created from client's credentials
     * @param catalog    catalog identifier (must be the DL account number)
     * @return HTTP session containing response results
     * @throws IOException in case of unsuccessful HTTP request
     */
    private static HttpURLConnection createUniverse(JWTTokenGenerator tokenMaker,
                                                    String catalog) throws IOException, ParseException {

        JSONParser parser = new JSONParser();
        new ConvertISINFF2JSON();
        logger.info("Converted ISIN list.txt to ISIN_LIST.json");
        Object obj = parser.parse(new FileReader("ISIN_LIST.json"));
        logger.info("Using ISIN_LIST.json for list of instruments to be passed");
        org.json.simple.JSONObject jsonObject = (org.json.simple.JSONObject) obj;

        org.json.simple.JSONArray isinList = (org.json.simple.JSONArray) jsonObject.get("ISINLIST");

        JSONObject payload = new JSONObject() {{
            put("@type", "Universe");
            put("identifier", UNIVERSE_ID);
            put("title", "FABCreditTradingUniverse1");
            put("description", "FAB credit trading universe");
            put("contains", isinList);
        }};


        String path = String.format(UNIVERSES_PATH_TEMPLATE, catalog);

        return httpPost(tokenMaker, path, CONTENT_TYPE_JSON, payload, MAX_REDIRECTS);

    }

    /**
     * Create a new field list resource.
     *
     * @param tokenMaker JWT token maker created from client's credentials
     * @param catalog    catalog identifier (must be the DL account number)
     * @return HTTP session containing response results
     * @throws IOException in case of unsuccessful HTTP request
     */
    private static HttpURLConnection createFieldList(JWTTokenGenerator tokenMaker,
                                                     String catalog) throws IOException {
        JSONObject payload = new JSONObject() {{
            put("@type", "DataFieldList");
            put("identifier", FIELD_LIST_ID);
            put("title", "FAB Credit Trading Field list1");
            put("description", "Field List for FAB Credit Trading Project");
            put("contains", new JSONArray() {{
                put(new JSONObject().put("cleanName", "name"));
                put(new JSONObject().put("cleanName", "classificationLevelName4"));
                put(new JSONObject().put("cleanName", "ticker"));
                put(new JSONObject().put("cleanName", "securityTyp"));
                put(new JSONObject().put("cleanName", "cntryIssueIso"));
                put(new JSONObject().put("cleanName", "crncy"));
                put(new JSONObject().put("cleanName", "series"));
                put(new JSONObject().put("cleanName", "cpnTyp"));
                put(new JSONObject().put("cleanName", "paymentRank"));
                put(new JSONObject().put("cleanName", "cpn"));
                put(new JSONObject().put("cleanName", "cpnFreq"));
                put(new JSONObject().put("cleanName", "issuePx"));
                put(new JSONObject().put("cleanName", "maturity"));
                put(new JSONObject().put("cleanName", "mtyTyp"));
                put(new JSONObject().put("cleanName", "issueSpreadBnchmrk"));
                put(new JSONObject().put("cleanName", "calcTypDes"));
                put(new JSONObject().put("cleanName", "securityPricingDate"));
                put(new JSONObject().put("cleanName", "firstSettleDt"));
                put(new JSONObject().put("cleanName", "firstCpnDt"));
                put(new JSONObject().put("cleanName", "idBb"));
                put(new JSONObject().put("cleanName", "idIsin"));
                put(new JSONObject().put("cleanName", "idBbGlobal"));
                put(new JSONObject().put("cleanName", "dayCntDes"));
                put(new JSONObject().put("cleanName", "bbComposite"));
                put(new JSONObject().put("cleanName", "amtOutstanding"));
                put(new JSONObject().put("cleanName", "amtIssued"));
                put(new JSONObject().put("cleanName", "minPiece"));
                put(new JSONObject().put("cleanName", "parAmt"));
                put(new JSONObject().put("cleanName", "leadMgr"));
                put(new JSONObject().put("cleanName", "exchCode"));

            }});
        }};

        String path = String.format(FIELD_LISTS_PATH_TEMPLATE, catalog);
        return httpPost(tokenMaker, path, CONTENT_TYPE_JSON, payload, MAX_REDIRECTS);
    }

    /**
     * Create a new trigger resource.
     *
     * @param tokenMaker JWT token maker created from client's credentials
     * @param catalog    catalog identifier (must be the DL account number)
     * @return HTTP session containing response results
     * @throws IOException in case of unsuccessful HTTP request
     */
    private static HttpURLConnection createTrigger(JWTTokenGenerator tokenMaker,
                                                   String catalog) throws IOException {
        // NOTE: The "SubmitTrigger" value is used as the trigger's type.
        //       We choose an event-based trigger, in this case, specifying that the
        //       job should run as soon as possible after POSTing the request component
        JSONObject payload = new JSONObject() {{
            put("@type", "SubmitTrigger");
            put("identifier", TRIGGER_ID);
            put("title", "FABCreditTradingTrigger1");
            put("description", "Run immediately after submitting request component/definition");
        }};

        // An alternative value, "ScheduledTrigger", can be used to specify a
        // recurring job schedule.
        // Using a 5 minute margin of safety for the job:
        // If the current time is less than 5 minutes before midnight in the account timezone,
        // Ensure the job will be processed the following day:
        /*
        final ZoneId accountTimeZone = ZoneId.of("America/New_York");
        final ZonedDateTime startDate = ZonedDateTime.now(accountTimeZone).plusMinutes(5);
        JSONObject payload = new JSONObject() {{
            put("@type", "ScheduledTrigger");
            put("identifier", TRIGGER_ID);
            put("title", "My Trigger");
            put("frequency", "daily");
            put("startDate", startDate.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
            put("startTime", "12:00:00");
        }};
         */

        String path = String.format(TRIGGERS_PATH_TEMPLATE, catalog);
        return httpPost(tokenMaker, path, CONTENT_TYPE_JSON, payload, MAX_REDIRECTS);
    }

    /**
     * Create a new request resource.
     * Note: only one request definition is allowed. It cannot be POSTed twice after the request resource has been created.
     *
     * @param tokenMaker   JWT token maker created from client's credentials
     * @param catalog      catalog identifier (must be the DL account number)
     * @param universeURL  URI of successfully created universe definition
     * @param fieldListURL URI of successfully created field list definition
     * @param triggerURL   URI of successfully created trigger definition
     * @return HTTP session containing response results
     * @throws IOException in case of unsuccessful HTTP request
     */
    private static HttpURLConnection createRequest(JWTTokenGenerator tokenMaker,
                                                   String catalog,
                                                   URL universeURL,
                                                   URL fieldListURL,
                                                   URL triggerURL) throws IOException {
        JSONObject payload = new JSONObject() {{
            put("@type", "DataRequest");
            put("identifier", REQUEST_ID);
            put("title", "FABCreditTradingReq1");
            put("description", "FAB Credit Trading Request");
            put("universe", universeURL);
            put("fieldList", fieldListURL);
            put("trigger", triggerURL);
            put("formatting", new JSONObject() {{
                put("@type", "DataFormat");
                put("columnHeader", true);
                put("dateFormat", "yyyymmdd");
                put("delimiter", "|");
                put("fileType", "unixFileType");
                put("outputFormat", "variableOutputFormat");
            }});
            put("pricingSourceOptions", new JSONObject() {{
                put("@type", "DataPricingSourceOptions");
                put("prefer", new JSONObject().put("mnemonic", "BGN"));
            }});
        }};

        String path = String.format(REQUESTS_PATH_TEMPLATE, catalog);
        return httpPost(tokenMaker, path, CONTENT_TYPE_JSON, payload, MAX_REDIRECTS);
    }

    /**
     * Print HTTP response content and return location of created resource.
     *
     * @param session HTTP session containing response results
     * @return URI of successfully created resource.
     * @throws IOException in case of unsuccessful response deserialization
     */
    private static URL handleResponse(HttpURLConnection session) throws IOException {
        try {

            logger.debug("Response: "+getJsonContent(session).toString(2));
            assert session.getResponseCode() >= 200 && session.getResponseCode() < 300;
            String location = session.getHeaderField("Location");

            if (location != null) {
                return new URL(PROTOCOL, HOST, session.getHeaderField("Location"));
            } else {
                return null;
            }
        } finally {
            session.disconnect();
        }
    }

    /**
     * Download content from provided HTTP session to file.
     *
     * @param tokenMaker         JWT token generator
     * @param location           location of requested HTTP resource
     * @param downloadedFilePath path of the file where to store the received content
     * @throws IOException in case if file IO operation failed or in case if content receive failed
     */
    private static void downloadDistribution(final JWTTokenGenerator tokenMaker,
                                             final URL location,
                                             final Path downloadedFilePath) throws IOException {

        final Map<String, String> httpHeaders = new HashMap<String, String>() {{
            put(HEADER_ACCEPT_ENCODING, ENCODING_GZIP);

        }};

        HttpURLConnection session = null;

        try {
            session = httpRequest(tokenMaker, "GET", location, httpHeaders, null, MAX_REDIRECTS);


            System.out.println();
            logger.info("Loading file from " + session.getURL());

            logger.debug("\tContent-Type = " + session.getContentType());
            logger.debug("\tContent-Disposition = " + session.getHeaderField("Content-Disposition"));
            logger.debug("\tContent-Length = " + session.getContentLength());
            // Create directories for storing the files
            File outputFile = new File(downloadedFilePath.toString());
            outputFile.getParentFile().mkdirs();
            // Copy file content from HTTP request to local file.
            try (final InputStream downloadedData = session.getInputStream()) {
                Files.copy(downloadedData, downloadedFilePath, StandardCopyOption.REPLACE_EXISTING);

            }
            logger.info("\tDistribution file stored at: " + downloadedFilePath);

        } finally {
            if (session != null) {
                session.disconnect();
            }
        }
    }

    /**
     * unzip Bloomberg data file with extension .bbg and places in current directory
     * @throws IOException
     */

    public static void unzipBBG() throws IOException {
        byte[] buffer = new byte[1024];
        String compressedFile = "SrcFiles/" + REQUEST_ID + ".bbg.gz";
        String unCompressedFile = "SrcFiles/DataLicense_SrcFile.txt";
        try {
            FileInputStream fileIn = new FileInputStream(compressedFile);
            GZIPInputStream gZIPInputStream = new GZIPInputStream(fileIn);
            FileOutputStream fileOutputStream = new FileOutputStream(unCompressedFile);
            int bytes_read;
            while ((bytes_read = gZIPInputStream.read(buffer)) > 0) {
                fileOutputStream.write(buffer, 0, bytes_read);
            }
            gZIPInputStream.close();
            fileOutputStream.close();
            logger.info("File Decompressed");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void removeHeaderTrailer() throws IOException {
        File f = new File("SrcFiles/DataLicense_SrcFile.txt");
        PrintWriter pw = new PrintWriter("SrcFiles/DL.txt");
        BufferedReader br = new BufferedReader(new FileReader(f));
        String line = null;
        String line1 = null;
        int startLine = 0;
        int endLine = 0;
        LineNumberReader lr = null;

        lr = new LineNumberReader(new FileReader(f));
        while ((line = lr.readLine()) != null) {
            if (line.startsWith("START-OF-DATA")) {
                startLine = lr.getLineNumber();
                logger.debug("Line number of start of data is : "+startLine);
            }
            if (line.startsWith("END-OF-DATA")) {
                endLine = lr.getLineNumber();
                logger.debug("Line number of end of data is: "+endLine);
            }
        }
        lr.close();

        int lnum = 0;
        LineNumberReader lr1 = new LineNumberReader(new FileReader(f));
        while ((line1 = lr1.readLine()) != null) {
            if ((lnum = lr1.getLineNumber()) > startLine && lnum < endLine) {
                pw.write(line1);
                pw.write("\n");
            }
        }


        logger.info("Source File for Informatica process created");
        pw.flush();
        pw.close();
        br.close();
        logger.debug("All print and buffer streams closed, no security loopholes here");

    }

    /**
     * Application entry point of this BEAP sample.
     *
     * @param args CLI arguments provided upon launching this application
     * @throws IOException propagated in case of socket or file system error
     */
    public static void main(String[] args) throws IOException, GeneralSecurityException {
        System.setProperty("https.proxyHost", "10.163.0.84");
        System.setProperty("https.proxyPort", "8080");
        PropertyConfigurator.configure("res/dl_logging.properties");
        logger.info("Using res/dl_logging.properties for logging properties");
        decryptFileName=args[0];
        password=args[1];

        EncryptFilewithPass.decryptFile(decryptFileName, password);
        JWTTokenGenerator tokenMaker = loadAuthenticator();
        final URL sseUri = new URL(PROTOCOL, HOST, NOTIFICATIONS_PATH);

        //  Create an SSE session to receive notification when reply is delivered
        try (final SSEClient sseClient = new SSEClient(sseUri, tokenMaker)) {
            final String catalog = loadAccountCatalog(tokenMaker);
            final URL universeURL = handleResponse(createUniverse(tokenMaker, catalog));
            final URL fieldListURL = handleResponse(createFieldList(tokenMaker, catalog));
            final URL triggerURL = handleResponse(createTrigger(tokenMaker, catalog));
            final URL requestURL = handleResponse(
                    createRequest(tokenMaker, catalog, universeURL, fieldListURL, triggerURL)
            );
            logger.debug(requestURL.toString());
            logger.info("Created request:");
            handleResponse(httpGet(tokenMaker, requestURL.getPath(), MAX_REDIRECTS));

            // Wait for notification that our output is ready for download. We allow a reasonable amount of time for
            // the request to be processed and avoid waiting forever for the purposes of the sample code -- a timeout
            // may not apply to your actual business workflow. For larger requests or requests made during periods of
            // high load, you may need to increase the timeout.
            ZonedDateTime start = ZonedDateTime.now(ZoneOffset.UTC);
            ZonedDateTime expirationTimestamp = start.plusMinutes(15);
            while (ZonedDateTime.now(ZoneOffset.UTC).compareTo(expirationTimestamp) < 0) {
                final SSEEvent sseEvent = sseClient.readEvent();
                if (sseEvent.isHeartbeat()) {
                    logger.info("Received heartbeat event, keep waiting for events");
                    continue;
                }

                final JSONObject notification = new JSONObject(sseEvent.getData());
                logger.info("Received reply delivery notification event: %s\n"+sseEvent.getData());

                try {
                    final JSONObject distribution = notification.getJSONObject("generated");
                    final String distributionId = distribution.getString("identifier");
                    final JSONObject notificationSnapshot = distribution.getJSONObject("snapshot");
                    final JSONObject notificationDataset = notificationSnapshot.getJSONObject("dataset");
                    final JSONObject notificationCatalog = notificationDataset.getJSONObject("catalog");
                    final String notificationCatalogId = notificationCatalog.getString("identifier");

                    if (!notificationCatalogId.equals(catalog) || !REPLY_ID.equals(distributionId)) {
                        logger.info("Some other delivery occurred - continue waiting");
                        continue;
                    }

                    // Download the distribution into a file.
                    final URL distributionURI = new URL(distribution.getString("@id"));

                    final Path filePath = Paths.get(
                            DOWNLOADS_DIRECTORY,
                            String.format(DOWNLOAD_FILE_TEMPLATE, distributionId)
                    );

                    downloadDistribution(tokenMaker, distributionURI, filePath);

                    logger.info("Reply was downloaded, unzip & exit now");
                    unzipBBG();
                    removeHeaderTrailer();
                    return;
                } catch (JSONException error) {
                    logger.error("Received other event type, continue waiting");
                }
            }
            logger.error("Reply NOT delivered, try to increase waiter loop timeout");

        } catch (SocketTimeoutException timeout) {
            logger.error("Connection timed out.");
            throw timeout;
        } catch (IOException error) {
            logger.error("HTTP connection error: " + error);
            throw error;
        } catch (InterruptedException error) {
            logger.error("Thread was interrupted, shutting down.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
