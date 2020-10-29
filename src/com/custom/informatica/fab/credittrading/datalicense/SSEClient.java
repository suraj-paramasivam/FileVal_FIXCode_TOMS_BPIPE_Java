package com.custom.informatica.fab.credittrading.datalicense;
/*
 * Copyright 2020. Bloomberg Finance L.P. Permission is hereby granted, free of
 * charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to
 * the following conditions: The above copyright notice and this permission
 * notice shall be included in all copies or substantial portions of the
 * Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */



import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This class represents a single parsed SSE event.
 */
class SSEEvent {
    private final String origin;
    private final String type;
    private final String id;
    private final String data;
    private final String comments;
    private final Integer retry;

    /**
     * Initializes a new instance of this class.
     *
     * @param origin   the URI this event came from
     * @param type     the type of this event
     * @param id       the identifier of this event
     * @param data     all the data lines of this event
     * @param comments all the comment lines of this event
     * @param retry    the retry timeout value contained in this event
     */
    SSEEvent(final String origin,
             final String type,
             final String id,
             final String data,
             final String comments,
             final Integer retry) {
        this.origin = origin;
        this.type = type;
        this.id = id;
        this.data = data;
        this.comments = comments;
        this.retry = retry;
    }

    /**
     * @return the URI this event came from
     */
    String getOrigin() {
        return origin;
    }

    /**
     * @return the type of this event
     */
    String getType() {
        return type;
    }

    /**
     * @return the identifier of this event
     */
    String getId() {
        return id;
    }

    /**
     * @return all the data lines of this event
     */
    String getData() {
        return data;
    }

    /**
     * @return all the comment lines of this event
     */
    String getComments() {
        return comments;
    }

    /**
     * @return the retry timeout value contained in this event
     */
    Integer getRetry() {
        return retry;
    }

    /**
     * @return true if this event is classified as a BEAP heartbeat, or false otherwise
     */
    boolean isHeartbeat() {
        return data == null;
    }
}

/**
 * This class constructs a new event from event stream lines.
 * A new instance of this class is supposed to be created for each event -
 * there is intentionally no reset method.
 */
class SSEEventBuilder {
    private static final Pattern fieldPattern = Pattern.compile("(?<field>event|id|data|retry):?( ?(?<value>.*))");

    private final String origin;
    private String type = "message";
    private String id;
    private final ArrayList<String> data = new ArrayList<>();
    private final ArrayList<String> comments = new ArrayList<>();
    private Integer retry;

    /**
     * Initializes a new instance of this class.
     *
     * @param origin the URI the event stream comes from
     */
    SSEEventBuilder(final String origin) {
        this.origin = origin;
    }

    /**
     * Parses the specified line according to the SSE standard rules
     * and makes the line a part of the event being constructed.
     *
     * @param line a single event stream line
     */
    void addLine(final String line) {
        if (line.startsWith(":")) {
            comments.add(line.substring(1));
            return;
        }

        final Matcher match = fieldPattern.matcher(line);
        if (!match.matches()) {
            // Unknown field names are ignored according to the standard.
            System.err.printf("Invalid SSE line: %s", line);
            System.err.println();
            return;
        }

        final String field = match.group("field");
        final String value = 1 < match.groupCount() ? match.group("value") : "";
        switch (field) {
            case "event":
                type = value;
                break;
            case "id":
                id = value;
                break;
            case "data":
                data.add(value);
                break;
            case "retry":
                try {
                    retry = Integer.parseUnsignedInt(value);
                } catch (NumberFormatException error) {
                    System.err.printf("Invalid retry timeout: %s", value);
                    System.err.println();
                }
                break;
        }
    }

    /**
     * Makes a new event using the fields collected so far.
     *
     * @return a new event
     */
    SSEEvent makeEvent() {
        return new SSEEvent(
                origin,
                type,
                id,
                data.isEmpty() ? null : String.join("\n", data),
                comments.isEmpty() ? null : String.join("\n", comments),
                retry);
    }
}

/**
 * This class is an SSE event stream parser.
 */
class SSEStreamParser implements Closeable {
    private final BufferedReader eventSource;
    private final String origin;

    /**
     * Initializes a new instance of this class.
     *
     * @param eventSource the event stream to be read
     * @param origin      the URI the event stream comes from
     */
    SSEStreamParser(final InputStream eventSource, final String origin) {
        this.eventSource = new BufferedReader(new InputStreamReader(eventSource, UTF_8));
        this.origin = origin;
    }

    /**
     * Reads an event from the event stream.
     *
     * @return the next event
     * @throws IOException in case the event stream is over
     */
    SSEEvent readEvent() throws IOException {
        final SSEEventBuilder builder = new SSEEventBuilder(origin);
        while (true) {
            final String line = eventSource.readLine();
            if (line == null) {
                // Discard the event if the stream ends before the final new line.
                throw new IOException("event stream is closed");
            }

            if (line.isEmpty()) {
                // Dispatch the event if a blank line is encountered.
                return builder.makeEvent();
            }

            // Keep collecting the event lines.
            builder.addLine(line);
        }
    }

    /**
     * Closes the underlying event stream.
     *
     * @throws IOException propagated from the underlying operation
     */
    @Override
    public void close() throws IOException {
        eventSource.close();
    }
}

/**
 * This class represents a connection to an SSE server.
 */
class SSESession implements Closeable {
    private static final int CONNECT_TIMEOUT_MS = 30000;
    private final HttpURLConnection connection;
    private final int status;

    /**
     * Initializes a new instance of this class.
     * Tries to establish a connection by sending an HTTP GET request.
     *
     * @param uri         a URI to send the initial request
     * @param tokenMaker  a JWT token maker created from client's credentials
     * @param lastEventID the latest received event identifier if any
     * @throws IOException propagated from the underlying operations
     */
    SSESession(
            final URL uri,
            final JWTTokenGenerator tokenMaker,
            final String lastEventID) throws IOException {
        final String METHOD = "GET";
        final String token = tokenMaker.createToken(uri, METHOD);
        connection = (HttpURLConnection) uri.openConnection();
        connection.setConnectTimeout(CONNECT_TIMEOUT_MS);
        connection.setInstanceFollowRedirects(false);
        connection.setReadTimeout(CONNECT_TIMEOUT_MS);
        connection.setRequestMethod(METHOD);
        connection.setRequestProperty("Accept", "text/event-stream");
        connection.setRequestProperty("Cache-Control", "no-cache");
        connection.setRequestProperty("JWT", token);
        if (!lastEventID.isEmpty()) {
            connection.setRequestProperty("Last-Event-ID", lastEventID);
        }
        status = connection.getResponseCode();
    }

    /**
     * @return true if the request has been successful, or false otherwise
     */
    boolean isOk() {
        return status == HttpURLConnection.HTTP_OK;  // 200
    }

    /**
     * @return true if a redirect is needed, or false otherwise
     */
    public boolean isRedirect() {
        final int HTTP_TEMPORARY_REDIRECT = 307;
        final int HTTP_PERMANENT_REDIRECT = 308;
        switch (status) {
            case HttpURLConnection.HTTP_MULT_CHOICE:  // 300
            case HttpURLConnection.HTTP_MOVED_PERM:   // 301
            case HttpURLConnection.HTTP_MOVED_TEMP:   // 302
            case HttpURLConnection.HTTP_SEE_OTHER:    // 303
            case HTTP_TEMPORARY_REDIRECT:             // 307
            case HTTP_PERMANENT_REDIRECT:             // 308
                return true;
            default:
                return false;
        }
    }

    /**
     * @return true if the request has been forbidden, or false otherwise
     */
    boolean isForbidden() {
        return status == HttpURLConnection.HTTP_FORBIDDEN;  // 403
    }

    /**
     * @return true if the response status indicates a server error, or false otherwise
     */
    boolean isServerError() {
        switch (status) {
            case HttpURLConnection.HTTP_INTERNAL_ERROR:   // 500
            case HttpURLConnection.HTTP_BAD_GATEWAY:      // 502
            case HttpURLConnection.HTTP_UNAVAILABLE:      // 503
            case HttpURLConnection.HTTP_GATEWAY_TIMEOUT:  // 504
                return true;
            default:
                return false;
        }
    }

    /**
     * @return the event stream
     * @throws IOException propagated from the underlying operation
     */
    InputStream getEventStream() throws IOException {
        return connection.getInputStream();
    }

    /**
     * Composes a URL using the new resource location.
     *
     * @return a new URL to redirect the request
     * @throws MalformedURLException propagated from a URL constructor
     */
    URL redirectURL() throws MalformedURLException {
        URL uri = connection.getURL();
        String location = connection.getHeaderField("Location");
        return new URL(uri.getProtocol(), uri.getHost(), location);
    }

    /**
     * Get error description from the response.
     * @return error description text
     * @throws IOException thrown in case if it was impossible to fetch the HTTP data
     */
    String getErrorString() throws IOException {
        InputStream errorStream = connection.getErrorStream();
        if (errorStream == null) {
            return connection.getResponseMessage();
        }

        InputStreamReader responseStream = new InputStreamReader(errorStream);
        try (BufferedReader input = new BufferedReader(responseStream)) {
            return input.lines().collect(Collectors.joining(System.lineSeparator()));
        }
    }

    /**
     * Closes the underlying connection.
     */
    @Override
    public void close() {
        connection.disconnect();
    }
}

/**
 * This class serves as a HTTP client capable of receiving events from a server.
 */
class SSEClient implements Closeable {
    private static final int MAX_ATTEMPTS = 3;
    private static final int MAX_REDIRECTS = 3;

    private final URL uri;
    private final JWTTokenGenerator tokenMaker;
    private SSESession session;
    private SSEStreamParser parser;
    private String lastEventId = "";
    private Integer retryTimeoutMs = 3000;

    /**
     * Initializes a new instance of this class.
     *
     * @param uri        a URI to send the initial request
     * @param tokenMaker a JWT token maker created from client's credentials
     * @throws IOException propagated from a failed connect
     */
    SSEClient(final URL uri, final JWTTokenGenerator tokenMaker) throws IOException {
        this.uri = uri;
        this.tokenMaker = tokenMaker;
        connect(uri, MAX_ATTEMPTS, MAX_REDIRECTS);
    }

    /**
     * Tries to establish a connection by sending a HTTP GET request.
     *
     * @param uri             a URI to send a connection request
     * @param attemptsRemain  the remaining number of additional attempts
     * @param redirectsRemain the remaining number of redirects
     * @throws IOException in case of unrecoverable errors
     */
    private void connect(final URL uri, final int attemptsRemain, final int redirectsRemain) throws IOException {
        if (attemptsRemain <= 0) {
            throw new IOException("too many connect attempts");
        }

        if (redirectsRemain <= 0) {
            throw new IOException("too many redirects");
        }

        close();
        try {
            session = new SSESession(uri, tokenMaker, lastEventId);
        } catch (IOException error) {
            System.err.println(error.getMessage());
            connect(uri, attemptsRemain - 1, redirectsRemain);
            return;
        }

        if (session.isOk()) {
            parser = new SSEStreamParser(session.getEventStream(), uri.toString());
            return;
        }

        if (session.isRedirect()) {
            connect(session.redirectURL(), attemptsRemain, redirectsRemain - 1);
            return;
        }

        if (session.isServerError()) {
            connect(uri, attemptsRemain - 1, redirectsRemain);
            return;
        }

        if (session.isForbidden()) {
            throw new IOException("either supplied credentials are invalid or expired," +
                    " or the requesting IP address is not on the allowlist");
        }

        throw new IOException("Server returned unexpected response: " + session.getErrorString());
    }

    /**
     * Reads an event from the event stream.
     *
     * @return the next event
     * @throws IOException propagated from the HTTP connectivity layer
     * @throws InterruptedException propagated from the Thread.sleep call
     */
    SSEEvent readEvent() throws InterruptedException, IOException {
        for (int i = 0; i < MAX_ATTEMPTS; ++i) {
            try {
                final SSEEvent event = parser.readEvent();
                if (event.getId() != null && !event.getId().isEmpty()) {
                    lastEventId = event.getId();
                }
                if (event.getRetry() != null) {
                    retryTimeoutMs = event.getRetry();
                }
                return event;
            } catch (Exception error) {
                System.err.printf("Error when reading event from the SSE server: %s", error.getMessage());
                System.err.println();
                if (i + 1 < MAX_ATTEMPTS) {
                    Thread.sleep(retryTimeoutMs);
                    connect(uri, MAX_ATTEMPTS, MAX_REDIRECTS);
                }
            }
        }
        throw new IOException("too many failed attempts to read an event");
    }

    /**
     * Releases all the underlying resources.
     *
     * @throws IOException propagated from the underlying operations
     */
    @Override
    public void close() throws IOException {
        if (session != null) {
            session.close();
        }
        if (parser != null) {
            parser.close();
        }
    }
}
