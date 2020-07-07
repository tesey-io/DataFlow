package org.tesey.dataflow.parser.endpoints.meta;

import java.util.ArrayList;
import java.util.Optional;

public class EndpointConfig {

    private String name;

    private String type;

    private String format;

    private String schemaPath;

    private ArrayList<EndpointConfigOption> options;

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return this.type;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getFormat() {
        return this.format;
    }

    public void setSchemaPath(String schemaPath) {
        this.schemaPath = schemaPath;
    }

    public String getSchemaPath() {
        return this.schemaPath;
    }

    public void setOptions(ArrayList<EndpointConfigOption> options) {
        this.options = options;
    }

    public ArrayList<EndpointConfigOption> getOptions() {
        return this.options;
    }

    public Optional<String> findOptionByNameOptional(String name) {
        return this.options.stream()
            .filter(o -> o.getName().equals(name))
            .map(o -> o.getValue())
            .findFirst();
    }

    public String findOptionByName(String name) {
        return this.findOptionByNameOptional(name).get();
    }

    public String findOptionByName(String name, String defaultVal) {
        return this.options.stream()
            .filter(o -> o.getName().equals(name))
            .map(o -> o.getValue())
            .findFirst()
            .orElse(defaultVal);
    }

}
