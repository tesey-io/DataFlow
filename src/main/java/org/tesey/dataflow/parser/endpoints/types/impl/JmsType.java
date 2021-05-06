package org.tesey.dataflow.parser.endpoints.types.impl;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.ExtendedJsonDecoder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jms.JmsIO;
import org.apache.beam.sdk.io.jms.JmsRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import net.sf.saxon.TransformerFactoryImpl;
import org.json.XML;
import com.ibm.mq.jms.MQConnectionFactory;
import org.apache.commons.io.IOUtils;
import org.tesey.dataflow.parser.endpoints.meta.EndpointConfig;
import org.tesey.dataflow.parser.endpoints.types.EndpointType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.stream.StreamSource;
import javax.xml.transform.stream.StreamResult;
import java.io.OutputStream;
import java.io.ByteArrayInputStream;

public class JmsType extends EndpointType {

    // JMS endpoint options
    private final static String  optionBrokerHost     = "brokerHost";
    private final static String  optionBrokerPort     = "brokerPort";
    private final static String  optionQueueManager   = "queueManager";
    private final static String  optionMessageChannel = "messageChannel";
    private final static String  optionTransportType  = "transportType";
    private final static String  optionJmsQueue       = "jmsQueue";
    private final static String  optionRecordTag      = "recordTag";
    private final static String  optionRootTag        = "rootTag";
    private final static String  xsltStylesheetPath   = "xsltStylesheetPath";
    private final static String  charset              = "charset";
    private final static Integer defaultTransportType = 1;

    public JmsType(EndpointConfig endpointConfig) {
        super(endpointConfig);
    }

    private static class ConvertGenericRecordToXmlFn extends DoFn<GenericRecord, String> {

        private final String recordTag;

        private final String rootTag;

        public ConvertGenericRecordToXmlFn(String recordTag, String rootTag){
            this.recordTag  = recordTag;
            this.rootTag    = rootTag;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            GenericRecord record = (GenericRecord) c.element();
            String result = XML.toString(XML.toString(record.toString(),recordTag), rootTag);
            c.output(result);
        }

    }



    private static class ExtractJmsRecordPayload extends DoFn<JmsRecord, String> {

        @ProcessElement
        public void processElement(
                @Element JmsRecord input,
                OutputReceiver<String> out) {
            out.output(input.getPayload());
        }

    }

    private static class ConvertXmlToJson extends DoFn<String, String> {

        private final String charset;

        private final String xsltString;

        private InputStream xslt;

        private Transformer transformer;

        ConvertXmlToJson(InputStream xslt, String charset) throws IOException {
            this.xsltString = IOUtils.toString(xslt);
            this.charset    = charset;
        }

        @Setup
        public void setup() throws TransformerConfigurationException {
            this.xslt        = new ByteArrayInputStream(xsltString.getBytes());
            this.transformer = new TransformerFactoryImpl().newInstance()
                    .newTransformer(new StreamSource(this.xslt));
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            String inputString = c.element();
            try (InputStream inputStream   = new ByteArrayInputStream(inputString.getBytes(charset));
                 OutputStream outputStream = new ByteArrayOutputStream()) {
                transformer.transform(new StreamSource(inputStream), new StreamResult(outputStream));
                xslt.reset();
                c.output(outputStream.toString());
            }
        }

    }

    private static class ConvertJsonToGenericRecord extends DoFn<String, GenericRecord> {

        private final String schemaJson;

        private Schema schema;

        ConvertJsonToGenericRecord(Schema schema){
            this.schemaJson = schema.toString();
        }

        @Setup
        public void setup(){
            schema = new Schema.Parser().parse(schemaJson);
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            String json = c.element();
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord> (schema);
            Decoder decoder = new ExtendedJsonDecoder(schema, json);
            GenericRecord record = reader.read(null,decoder);
            c.output(record);
        }
    }





    public PCollection<GenericRecord> getSourceCollection(Pipeline pipeline, Schema schema) throws Exception {

        MQConnectionFactory connectionFactory = new MQConnectionFactory();

        connectionFactory
                .setTransportType(this.endpointConfig.findOptionByNameOptional(this.optionTransportType).isPresent()
                        ? Integer.valueOf(this.endpointConfig.findOptionByName(this.optionTransportType))
                        : this.defaultTransportType);
        connectionFactory.setHostName(this.endpointConfig.findOptionByName(this.optionBrokerHost));
        connectionFactory.setPort(Integer.valueOf(this.endpointConfig.findOptionByName(this.optionBrokerPort)));
        connectionFactory.setQueueManager(this.endpointConfig.findOptionByName(this.optionQueueManager));
        connectionFactory.setChannel(this.endpointConfig.findOptionByName(this.optionMessageChannel));

        JmsIO.Read<JmsRecord> jmsIORead = JmsIO.<JmsRecord>read()
                .withConnectionFactory(connectionFactory)
                .withQueue(this.endpointConfig.findOptionByName(this.optionJmsQueue));

        PCollection<GenericRecord> records = null;

        PCollection<String> resultCollection = pipeline
            .apply(String.format("Endpoint `%s`: Ingest data", this.endpointConfig.getName()),
                jmsIORead)
            .apply(String.format("Endpoint `%s`: Get message payload", this.endpointConfig.getName()),
                ParDo.of(new ExtractJmsRecordPayload()));

        switch (endpointConfig.getFormat()) {
            case "xml":

                try (InputStream xslt = JmsType.class.getClassLoader()
                    .getResourceAsStream(this.endpointConfig.findOptionByName(this.xsltStylesheetPath))) {

                    PCollection<String> json = resultCollection.apply(
                        String.format("Endpoint `%s`: Transform XML to JSON", this.endpointConfig.getName()),
                        ParDo.of(new ConvertXmlToJson(xslt,
                            this.endpointConfig.findOptionByName(this.charset))));

                    records = resultCollection
                        .apply(String.format("Endpoint `%s`: Convert JSON to GenericRecord",
                                this.endpointConfig.getName()),
                                ParDo.of(new ConvertJsonToGenericRecord(schema)));
                }

                break;

            default:
                throw new Exception(
                        String.format("Unknown format `%s` specified for the endpoint `%s`",
                                endpointConfig.getFormat(), this.endpointConfig.getName()));
        }

        return records;

    }

    public PTransform<PCollection<String>, PDone> buildWriteTransform() throws Exception {

        MQConnectionFactory connectionFactory = new MQConnectionFactory();

        connectionFactory.setTransportType(1);
        connectionFactory.setHostName(this.endpointConfig.findOptionByName(this.optionBrokerHost));
        connectionFactory.setPort(Integer.valueOf(this.endpointConfig.findOptionByName(this.optionBrokerPort)));
        connectionFactory.setQueueManager(this.endpointConfig.findOptionByName(this.optionQueueManager));
        connectionFactory.setChannel(this.endpointConfig.findOptionByName(this.optionMessageChannel));

        return createWriteTransform()
            .withConnectionFactory(connectionFactory)
            .withQueue(this.endpointConfig.findOptionByName(this.optionJmsQueue));

    }

    private JmsIO.Write createWriteTransform() throws Exception {

        JmsIO.Write writeTransform = JmsIO.write();

        return writeTransform;

    }

    public void writeGenericRecords(PCollection<GenericRecord> records, Schema schema) throws Exception {
        switch (endpointConfig.getFormat()) {
            case "xml":
                records
                        .apply(String.format("Endpoint `%s`: Convert GenericRecord to Bytes", this.endpointConfig.getName()),
                                ParDo.of(new JmsType.ConvertGenericRecordToXmlFn(
                                        this.endpointConfig.findOptionByName(this.optionRecordTag),
                                        this.endpointConfig.findOptionByName(this.optionRootTag))))
                        .apply(String.format("Endpoint `%s`: Write GenericRecord", this.endpointConfig.getName()),
                                buildWriteTransform());
                break;

            default:
                throw new Exception(
                        String.format("Unknown format `%s` specified for the endpoint `%s`",
                                endpointConfig.getFormat(), this.endpointConfig.getName()));
        }
    }

}
