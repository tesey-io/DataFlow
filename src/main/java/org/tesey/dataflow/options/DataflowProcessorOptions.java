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
