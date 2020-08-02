package com.custom.informatica.fab.credittrading.toms;

import org.apache.commons.io.FileUtils;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.List;
import java.util.ArrayList;
import java.net.URL;
import java.net.URLConnection;
import java.net.HttpURLConnection;
import java.io.InputStream;
import java.io.OutputStream;


public class LongPollingKafkaPublisher {
    private static final String HTTPSTART = "HTTP/1.1";

    private String _url;
    private URLConnection _connection;
    private URL _event;
    private InputStream _in;

    public LongPollingKafkaPublisher(String url)
            throws Exception
    {

        _url = url;
        open();
    }

    private void open()
            throws Exception
    {
        _event = new URL(_url);
        System.out.println("Protocol is: " + _event.getProtocol());
        _connection = _event.openConnection();
        _connection.setDoOutput(true);
    }

    public void close()
            throws Exception
    {
        if (_in != null)
            _in.close();
    }

    public void writeRequest(String post)
            throws Exception
    {
        //((HttpURLConnection)_connection).setFixedLengthStreamingMode(post.length());
        OutputStream stream = _connection.getOutputStream();
        stream.write(post.getBytes());
        stream.close();
        System.out.println(":Sent request:" + _url);
    }

    public void getResponses()
            throws Exception
    {
        if (_in == null)
            _in = _connection.getInputStream();

        byte[] bytes = new byte[1024];

        StringBuffer buff = new StringBuffer();
        int status = ((HttpURLConnection)_connection).getResponseCode();
        int len = ((HttpURLConnection)_connection).getContentLength();
        InputStream istr = (InputStream)_connection.getContent();
        System.out.println("Trying to read: " + status + ":" + len );
        int read = istr.read(bytes, 0, bytes.length);
        while (read > 0)
        {
            String str = new String(bytes, 0, read);
            buff.append(str);
            read = istr.read(bytes, 0, bytes.length);
        }

        System.out.println("Received: " + buff.toString() + ":" + status);
    }

    public static void main(String[] args)
            throws Exception
    {
        LongPollingKafkaPublisher clnt = new LongPollingKafkaPublisher("https://beta.api.bloomberg.com");
        clnt.writeRequest("Just Testing here.");
        clnt.getResponses();
        clnt.close();

    }

}
