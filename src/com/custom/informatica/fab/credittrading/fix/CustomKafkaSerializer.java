package com.custom.informatica.fab.credittrading.fix;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import java.util.logging.Logger;
import java.util.logging.FileHandler;
public class CustomKafkaSerializer <T extends SpecificRecordBase> implements Serializer<T> {

    FileHandler handler = new FileHandler("CustomKafkaSerializer.log", true);
    Logger logger =  Logger.getLogger("com.custom.informatica.fab.credittrading.fix.CustomKafkaSerializer");

    public CustomKafkaSerializer() throws IOException {
    }

    @Override
    public void close() {
        // No-op
    }

    @Override
    public void configure(Map<String, ?> arg0, boolean arg1) {
        // No-op
    }

    public byte[] serialize(String topic, T data) {
        try {
            byte[] result = null;

            if (data != null) {
                logger.finest("data="+data);

                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                BinaryEncoder binaryEncoder =
                        EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);

                DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(data.getSchema());
                datumWriter.write(data, binaryEncoder);

                binaryEncoder.flush();
                byteArrayOutputStream.close();

                result = byteArrayOutputStream.toByteArray();
                logger.finest("serialized data="+ DatatypeConverter.printHexBinary(result));
            }
            return result;
        } catch (IOException ex) {
            throw new SerializationException(
                    "Can't serialize data='" + data + "' for topic='" + topic + "'", ex);
        }

    }
}
