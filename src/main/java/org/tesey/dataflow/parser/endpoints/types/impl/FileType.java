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
