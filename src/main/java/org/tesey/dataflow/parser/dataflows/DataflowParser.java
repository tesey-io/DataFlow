package org.tesey.dataflow.parser.dataflows;

import org.tesey.dataflow.parser.dataflows.meta.DataflowConfig;
import org.tesey.dataflow.parser.dataflows.meta.DataflowConfigs;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class DataflowParser {

    private HashMap<String, DataflowConfig> dataflows;

    private String firstDataflow;

    public DataflowParser(String configFilePath) throws IOException {

        try (InputStream inputStream = DataflowParser.class.getClassLoader()
            .getResourceAsStream(configFilePath)) {

            Yaml yaml = new Yaml(new Constructor(DataflowConfigs.class));

            DataflowConfigs dataflowConfigs = yaml.loadAs(inputStream, DataflowConfigs.class);

            this.dataflows = (HashMap<String, DataflowConfig>) dataflowConfigs.getDataflows().stream()
                .filter(p -> p instanceof DataflowConfig)
                .map(e -> (DataflowConfig) e)
                .collect(Collectors.toMap(DataflowConfig::getName, e -> e));

        }

    }

    public HashMap<String, DataflowConfig> getDataflows() {
        return this.dataflows;
    }

    public String getFirstDataflow() throws Exception {

        if (this.firstDataflow == null || this.firstDataflow == "") {

            Optional<Map.Entry<String, DataflowConfig>> optionalEntry = this.dataflows.entrySet()
                .stream()
                .filter(e -> e.getValue().getIsFirst())
                .findFirst();

            if (optionalEntry.isPresent()) {

                this.firstDataflow = optionalEntry.get().getKey();

            } else {
                throw new Exception("Invalidate configuration: please denote the dataflow that should be chosen "
                    + "the first using option `isFirst`");
            }

        }

        return this.firstDataflow;

    }

    public String getSuccessorName(String dataflowName) {

        Optional<Map.Entry<String, DataflowConfig>> optionalEntry = this.dataflows.entrySet()
            .stream()
            .filter(e -> (e.getValue().getSource() != null && e.getValue().getSource() != ""
                && e.getValue().getSource().equals(dataflowName)))
            .findFirst();

        return optionalEntry.isPresent() ? optionalEntry.get().getKey() : "";

    }

}
