package org.tesey.dataflow.parser.endpoints;

import org.tesey.dataflow.parser.endpoints.meta.EndpointConfig;
import org.tesey.dataflow.parser.endpoints.meta.EndpointConfigs;
import org.tesey.dataflow.parser.endpoints.types.EndpointType;
import org.tesey.dataflow.parser.endpoints.types.impl.FileType;
import org.tesey.dataflow.parser.endpoints.types.impl.KafkaType;
import org.apache.avro.Schema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.stream.Collectors;

public class EndpointParser {

    private HashMap<String, EndpointContext> endpoints;

    private static Logger log = Logger.getLogger(EndpointParser.class);

    public class EndpointContext {

        private EndpointConfig endpointConfig;

        private Schema schema;

        private EndpointType endpointType;

        public EndpointContext(EndpointConfig endpointConfig) {

            this.endpointConfig = endpointConfig;

        }

        private Schema getSchema() {

            if (this.schema == null)
            try (InputStream schemaStream = EndpointParser.class.getClassLoader()
                .getResourceAsStream(this.endpointConfig.getSchemaPath())) {

                Schema.Parser parser = new Schema.Parser();

                this.schema = parser.parse(schemaStream);

            } catch (IOException e) {
                log.error(String.format("File %s not found", this.endpointConfig.getSchemaPath()));
            }

            return this.schema;

        }

        private EndpointType getEndpointType() throws Exception {

            if (this.endpointType == null)
            switch (endpointConfig.getType()) {
                case "kafka":
                    this.endpointType = new KafkaType(endpointConfig);
                    break;
                case "file":
                    this.endpointType = new FileType(endpointConfig);
                    break;
                default:
                    throw new Exception(String.format("Endpoint `%s`: Unknown endpoint type `%s`",
                        this.endpointConfig.getName(), endpointType));
            }

            return this.endpointType;

        }

        public PCollection<Row> getRows(Pipeline pipeline) throws Exception {

            return getEndpointType().readRows(pipeline, this.getSchema());

        }

        public void writeRows(PCollection<Row> rows) throws Exception {

            getEndpointType().writeRows(rows, getSchema());

        }

    }

    public EndpointParser(String configFilePath) throws IOException {

        try (InputStream endpointStream = EndpointParser.class.getClassLoader()
            .getResourceAsStream(configFilePath)) {

            Yaml yaml = new Yaml(new Constructor(EndpointConfigs.class));

            EndpointConfigs endpointConfigs = yaml.loadAs(endpointStream, EndpointConfigs.class);

            this.endpoints = (HashMap<String, EndpointContext>) endpointConfigs.getEndpoints().stream()
                .filter(p -> p instanceof EndpointConfig)
                .map(e -> (EndpointConfig) e)
                .collect(Collectors.toMap(EndpointConfig::getName, e -> new EndpointContext(e)));

        }

    }

    public HashMap<String, EndpointContext> getEndpoints() {
        return this.endpoints;
    }

}
