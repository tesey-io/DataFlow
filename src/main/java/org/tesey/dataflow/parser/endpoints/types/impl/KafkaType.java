/*
 * Copyright 2020 The Tesey Software Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tesey.dataflow.parser.endpoints.types.impl;

import org.tesey.dataflow.parser.endpoints.meta.EndpointConfig;
import org.tesey.dataflow.parser.endpoints.types.EndpointType;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class KafkaType extends EndpointType {

    // Kafka endpoint options
    private final static String optionTopicName = "topic";
    private final static String optionBootstrapServersName = "bootstrapServers";
    private final static String optionAutoOffsetResetName = "autoOffsetReset";

    public KafkaType(EndpointConfig endpointConfig) {
        super(endpointConfig);
    }

    private static class ConvertBytesToGenericRecordFn extends DoFn<KV<String, byte[]>, GenericRecord> {

        private final String schemaJson;

        private Schema schema;

        ConvertBytesToGenericRecordFn(Schema schema){
            schemaJson = schema.toString();
        }

        @Setup
        public void setup(){
            schema = new Schema.Parser().parse(schemaJson);
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            KV<String, byte[]> kv = (KV<String, byte[]>) c.element();
            byte[] bf = kv.getValue();
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord> (schema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(bf,null);
            GenericRecord record = reader.read(null,decoder);
            c.output(record);
        }

    }

    private static class ConvertGenericRecordToByteArrayFn extends DoFn<GenericRecord, byte[]> {

        private final String schemaJson;

        private Schema schema;

        public ConvertGenericRecordToByteArrayFn(Schema schema){
            schemaJson = schema.toString();
        }

        @Setup
        public void setup(){
            schema = new Schema.Parser().parse(schemaJson);
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            GenericRecord record = (GenericRecord) c.element();
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);
            writer.write(record, encoder);
            encoder.flush();
            c.output(output.toByteArray());
        }

    }

    private Class<? extends Serializer<?>> getValueDeserializer() throws Exception {
        Class<? extends Serializer<?>> serializer = null;
        switch (endpointConfig.getFormat()) {
            case "avro":
                serializer = (Class) ByteArrayDeserializer.class;
                break;
            default:
                throw new Exception(
                    String.format("Unknown format `%s` specified for the endpoint `%s`",
                        endpointConfig.getFormat(), this.endpointConfig.getName()));
        }
        return serializer;
    }

    private <K, V> KafkaIO.Read<K, V> createReadTransform() throws Exception {

        KafkaIO.Read<K, V> readTransform = null;

        switch (endpointConfig.getFormat()) {
            case "avro":
                readTransform = (KafkaIO.Read<K, V>) KafkaIO.<K, byte[]> read();
                break;
            default:
                throw new Exception(
                    String.format("Unknown format `%s` specified for the endpoint `%s`",
                        endpointConfig.getFormat(), this.endpointConfig.getName()));
        }

        return readTransform;

    }

    public <T> PTransform<PBegin, PCollection<T>> buildReadTransform() throws Exception {

        Map<String, Object> consumerProperties = ImmutableMap.of(
        "auto.offset.reset", this.endpointConfig.findOptionByName(this.optionAutoOffsetResetName, "earliest")
        );

        return createReadTransform()
            .withBootstrapServers(this.endpointConfig.findOptionByName(this.optionBootstrapServersName))
            .withTopic(this.endpointConfig.findOptionByName(this.optionTopicName))
            .withKeyDeserializer((Class) StringDeserializer.class)
            .withValueDeserializer(getValueDeserializer())
            .updateConsumerProperties(consumerProperties)
            .withoutMetadata();

    }

    public PCollection<GenericRecord> getSourceCollection(Pipeline pipeline, Schema schema) throws Exception {

        PCollection<GenericRecord> records = null;

        switch (endpointConfig.getFormat()) {
            case "avro":

                PTransform<PBegin, PCollection<KV<String, byte[]>>> kafkaBytesReadTransform = this.buildReadTransform();

                PCollection<KV<String, byte[]>> resultCollection = pipeline
                    .apply(String.format("Endpoint `%s`: Ingest data", this.endpointConfig.getName()),
                        kafkaBytesReadTransform);

                records = resultCollection
                    .apply(String.format("Endpoint `%s`: Convert ingested data ingested to GenericRecord",
                        this.endpointConfig.getName()), ParDo.of(new ConvertBytesToGenericRecordFn(schema)))
                    .setCoder(AvroCoder.of(GenericRecord.class, schema));

                break;

            default:
                throw new Exception(
                    String.format("Unknown format `%s` specified for the endpoint `%s`",
                        endpointConfig.getFormat(), this.endpointConfig.getName()));
        }

        return records;

    }

    private Class<? extends Serializer<?>> getValueSerializer() throws Exception {
        Class<? extends Serializer<?>> serializer = null;
        switch (endpointConfig.getFormat()) {
            case "avro":
                serializer = (Class) ByteArraySerializer.class;
                break;
            default:
                throw new Exception(
                    String.format("Unknown format `%s` specified for the endpoint `%s`",
                        endpointConfig.getFormat(), this.endpointConfig.getName()));
        }
        return serializer;
    }

    private <K, V> KafkaIO.Write<K, V> createWriteTransform() throws Exception {

        KafkaIO.Write<K, V> writeTransform = null;

        switch (endpointConfig.getFormat()) {
            case "avro":
                writeTransform = (KafkaIO.Write<K, V>) KafkaIO.<K, byte[]> write();
                break;
            default:
                throw new Exception(
                    String.format("Unknown format `%s` specified for the endpoint `%s`",
                        endpointConfig.getFormat(), this.endpointConfig.getName()));
        }

        return writeTransform;

    }

    public <T> PTransform<PCollection<T>, PDone> buildWriteTransform() throws Exception {

        return createWriteTransform()
            .withBootstrapServers(this.endpointConfig.findOptionByName(this.optionBootstrapServersName))
            .withTopic(this.endpointConfig.findOptionByName(this.optionTopicName))
            .withValueSerializer((Class) getValueSerializer())
            .values();

    }

    public void writeGenericRecords(PCollection<GenericRecord> records, Schema schema) throws Exception {
        switch (endpointConfig.getFormat()) {
            case "avro":
                records
                    .apply(String.format("Endpoint `%s`: Convert GenericRecord to Bytes", this.endpointConfig.getName()),
                        ParDo.of(new ConvertGenericRecordToByteArrayFn(schema)))
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
