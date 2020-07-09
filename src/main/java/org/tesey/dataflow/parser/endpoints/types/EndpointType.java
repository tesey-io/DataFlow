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

package org.tesey.dataflow.parser.endpoints.types;

import org.tesey.dataflow.parser.endpoints.meta.EndpointConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.io.IOException;

public abstract class EndpointType {

    protected EndpointConfig endpointConfig;

    protected static class ConvertGenericRecordToRowFn extends DoFn<GenericRecord, Row> {

        private final String schemaJson;

        private Schema schema;

        public ConvertGenericRecordToRowFn(Schema schema){
            schemaJson = schema.toString();
        }

        @Setup
        public void setup(){
            schema = new Schema.Parser().parse(schemaJson);
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {

            GenericRecord record = (GenericRecord) c.element();

            Row row = AvroUtils.toBeamRowStrict(record,AvroUtils.toBeamSchema(schema));

            c.output(row);

        }

    }

    protected static class ConvertRowToGenericRecordFn extends DoFn<Row, GenericRecord> {

        private final String schemaJson;

        private Schema schema;

        public ConvertRowToGenericRecordFn(Schema schema){
            schemaJson = schema.toString();
        }

        @Setup
        public void setup(){
            schema = new Schema.Parser().parse(schemaJson);
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {

            Row row = (Row) c.element();

            GenericRecord record = AvroUtils.toGenericRecord(row,schema);

            c.output(record);

        }

    }

    public abstract PCollection<GenericRecord> getSourceCollection(Pipeline pipeline, Schema schema) throws Exception;

    public EndpointType (EndpointConfig endpointConfig) {
        this.endpointConfig = endpointConfig;
    }

    public PCollection<Row> readRows(Pipeline pipeline, Schema schema) throws Exception {

        PCollection<GenericRecord> records = getSourceCollection(pipeline, schema);

        PCollection<Row> rows = records
            .apply(String.format("Endpoint `%s`: Convert GenericRecord to Row", this.endpointConfig.getName()),
                ParDo.of(new ConvertGenericRecordToRowFn(schema))).setRowSchema(AvroUtils.toBeamSchema(schema));

        return rows;

    }

    public abstract void writeGenericRecords(PCollection<GenericRecord> records, Schema schema) throws Exception;

    public void writeRows(PCollection<Row> rows, Schema schema) throws Exception {
        PCollection<GenericRecord> records = rows
            .apply(String.format("Endpoint `%s`: Convert Row to GenericRecord", this.endpointConfig.getName()),
                ParDo.of(new ConvertRowToGenericRecordFn(schema))).setCoder(AvroCoder.of(schema));

        this.writeGenericRecords(records, schema);
    }

}
