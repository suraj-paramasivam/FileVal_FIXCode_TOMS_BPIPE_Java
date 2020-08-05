package com.custom.informatica.fab.credittrading.common;
import okhttp3.*;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.io.IOException;
public class KafkaSchemaPublisher {
    private final static MediaType SCHEMA_CONTENT = MediaType.parse("application/vnd.schemaregistry.v1+json");
    private final static String BGCGFIPRICING_SCHEMA = "{\n" +
            "  \"schema\": \"" +
            "  {" +
            "    \\\"namespace\\\": \\\"com.custom.informatica.fab.credittrading.fix.bgcgfi\\\"," +
            "    \\\"type\\\": \\\"record\\\"," +
            "    \\\"name\\\": \\\"fact_bgc_gfi_pricing\\\"," +
            "    \\\"fields\\\": [" +
            "        {\\\"name\\\": \\\"MSGTYPE\\\", \\\"type\\\": \\\"string\\\"}," +
            "        {\\\"name\\\": \\\"MSGSEQNUM\\\", \\\"type\\\": \\\"int\\\"}," +
            "        {\\\"name\\\": \\\"SENDERCOMPID\\\",  \\\"type\\\": \\\"string\\\"}," +
            "        {\\\"name\\\": \\\"SENDINGTIME\\\",  \\\"type\\\": \\\"string\\\"}," +
            "        {\\\"name\\\": \\\"TARGETCOMPID\\\",  \\\"type\\\": \\\"string\\\"}," +
            "        {\\\"name\\\": \\\"MDREQID\\\",  \\\"type\\\": \\\"string\\\"}," +
            "        {\\\"name\\\": \\\"MDUPDATEACTION\\\",  \\\"type\\\": \\\"string\\\"}," +
            "        {\\\"name\\\": \\\"FULLSYMBOL\\\",  \\\"type\\\": \\\"string\\\"}," +
            "        {\\\"name\\\": \\\"TYPEOFINSTRUMENT\\\",  \\\"type\\\": \\\"string\\\"}," +
            "        {\\\"name\\\": \\\"QUOTATIONMETHOD\\\",  \\\"type\\\": \\\"string\\\"}," +
            "        {\\\"name\\\": \\\"ISIN\\\",  \\\"type\\\": \\\"string\\\"}," +
            "        {\\\"name\\\": \\\"REFPRICEQUOTEDTYPE\\\",  \\\"type\\\": \\\"string\\\"}," +
            "        {\\\"name\\\": \\\"MDENTRYPX\\\",  \\\"type\\\": \\\"int\\\"}," +
            "        {\\\"name\\\": \\\"MDENTRYSIZE\\\",  \\\"type\\\": \\\"string\\\"}," +
            "        {\\\"name\\\": \\\"MDENTRYDATE\\\",  \\\"type\\\": \\\"int\\\"}," +
            "        {\\\"name\\\": \\\"MDENTRYTIME\\\",  \\\"type\\\": \\\"string\\\"}" +
            "    ]" +
            "  }\"" +
            "}";

  }
