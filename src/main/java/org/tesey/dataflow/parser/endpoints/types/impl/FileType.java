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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.values.PCollection;

public class FileType extends EndpointType {

    // File endpoint options
    private final static String optionPathToDataset = "pathToDataset";

    public FileType(EndpointConfig endpointConfig) {
        super(endpointConfig);
    }

    public PCollection<GenericRecord> getSourceCollection(Pipeline pipeline, Schema schema) throws Exception {

        PCollection<GenericRecord> records = null;

        switch (endpointConfig.getFormat()) {
            case "avro":
                records = pipeline
                    .apply(String.format("Endpoint `%s`: Ingest data", this.endpointConfig.getName()),
                        AvroIO.readGenericRecords(schema)
                            .from(this.endpointConfig.findOptionByName(optionPathToDataset)));

                break;

            case "parquet":
                PCollection<FileIO.ReadableFile> parquetFiles = pipeline
                    .apply("Endpoint `%s`: Match filepattern",
                        FileIO.match().filepattern(this.endpointConfig.findOptionByName(optionPathToDataset)))
                    .apply("Endpoint `%s`: Convert matching results to FileIO.ReadableFile", FileIO.readMatches());

                records = parquetFiles
                    .apply(String.format("Endpoint `%s`: Ingest data", this.endpointConfig.getName()),
                        ParquetIO.readFiles(schema));

                break;

            default:
                throw new Exception(
                    String.format("Unknown format `%s` specified for the endpoint `%s`",
                        endpointConfig.getFormat(), this.endpointConfig.getName()));
        }

        return records;

    }

    public void writeGenericRecords(PCollection<GenericRecord> records, Schema schema) throws Exception {
        switch (endpointConfig.getFormat()) {
            case "avro":
                records
                    .apply(String.format("Endpoint `%s`: Write GenericRecord's", this.endpointConfig.getName()),
                        AvroIO.writeGenericRecords(schema)
                            .to(this.endpointConfig.findOptionByName(optionPathToDataset)));

                break;

            case "parquet":
                records
                    .apply(String.format("Endpoint `%s`: Write GenericRecord's", this.endpointConfig.getName()),
                        FileIO.<GenericRecord>write()
                            .via(ParquetIO.sink(schema))
                            .to(this.endpointConfig.findOptionByName(optionPathToDataset)));
                break;

            default:
                throw new Exception(
                    String.format("Unknown format `%s` specified for the endpoint `%s`",
                        endpointConfig.getFormat(), this.endpointConfig.getName()));
        }
    }

}
