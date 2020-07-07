package org.tesey.dataflow.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface DataflowProcessorOptions extends PipelineOptions {

    @Description("Path to endpoints configuration YAML file")
    String getEndpointConfigFilePath();

    void setEndpointsConfigFilePath(String value);

    @Description("Path to dataflows configuration YAML file")
    String getDataflowConfigFilePath();

    void setDataflowsConfigFilePath(String value);

}
