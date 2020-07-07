package org.tesey.dataflow;

import org.tesey.dataflow.options.DataflowProcessorOptions;
import org.tesey.dataflow.parser.dataflows.DataflowParser;
import org.tesey.dataflow.parser.dataflows.meta.DataflowConfig;
import org.tesey.dataflow.parser.endpoints.EndpointParser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

import java.util.HashMap;

public class DataflowProcessor {

    private static final String simpleQueryTemplate   = "SELECT %s FROM PCOLLECTION";

    private static final String queryWithJoinTemplate = "SELECT %s FROM %s INNER JOIN %s ON %s";

    private static void registerEndpoint(Pipeline pipeline, HashMap<String, PCollection<Row>> collectionContext,
                                         HashMap<String, EndpointParser.EndpointContext> endpoints,
                                         String endpointName) throws Exception {

        if (!collectionContext.containsKey(endpointName)) {

            EndpointParser.EndpointContext endpointContext = endpoints.get(endpointName);

            collectionContext.put(endpointName, endpointContext.getRows(pipeline));

        }

    }

    private static void addSuccessor(Pipeline pipeline, HashMap<String, PCollection<Row>> collectionContext,
                                     String dataflownName, HashMap<String, EndpointParser.EndpointContext> endpoints,
                                     DataflowParser dataflowParser) throws Exception {

        String successorName = dataflowParser.getSuccessorName(dataflownName);

        if (successorName != null && successorName != "") {

            composePipeline(pipeline, collectionContext, successorName, endpoints, dataflowParser);

        }

    }

    private static String getSelect(String select) throws Exception {

        return select != null && select != "" ? select : "*";

    }

    private static String getFilter(String filter) throws Exception {

        return filter != null && filter != "" ? String.format(" WHERE %s ", filter) : "";

    }

    private static String getGroupBy(String groupBy) throws Exception {

        return groupBy != null && groupBy != "" ? String.format(" GROUP BY %s ", groupBy) : "";

    }

    private static void composePipeline(Pipeline pipeline,
                                        HashMap<String, PCollection<Row>> collectionContext,
                                        String dataflowName,
                                        HashMap<String, EndpointParser.EndpointContext> endpoints,
                                        DataflowParser dataflowParser) throws Exception {

        if (endpoints.containsKey(dataflowName)) {

            registerEndpoint(pipeline, collectionContext, endpoints, dataflowName);

            addSuccessor(pipeline, collectionContext, dataflowName, endpoints, dataflowParser);

        } else if (dataflowParser.getDataflows().containsKey(dataflowName)) {

            DataflowConfig dataflowConfig = dataflowParser.getDataflows().get(dataflowName);

            if (dataflowConfig.getSource() != null && dataflowConfig.getSource() != "") {

                registerEndpoint(pipeline, collectionContext, endpoints, dataflowConfig.getSource());

                PCollection<Row> table = collectionContext.get(dataflowConfig.getSource());

                if (dataflowConfig.getWindow() != null){

                    table = table
                        .apply(String.format("Set window size for dataflow `%s`", dataflowName),
                            Window.<Row>into(FixedWindows.of(Duration.standardSeconds(dataflowConfig.getWindow()))));

                }

                if (dataflowConfig.getJoin() != null) {

                    String rightDataflowName = dataflowConfig.getJoin().getDataflow();

                    DataflowConfig rightDataflowConfig = dataflowParser.getDataflows().get(rightDataflowName);

                    if (dataflowConfig.getWindow() == null && rightDataflowConfig.getWindow() != null) {
                        throw new Exception(String.format("Please specify window size for joining dataflow `%s`",
                            dataflowConfig.getName()));
                    }

                    if (dataflowConfig.getWindow() != null && rightDataflowConfig.getWindow() == null) {
                        throw new Exception(String.format("Please specify window size for joining dataflow `%s`",
                            rightDataflowConfig.getName()));
                    }

                    if (!dataflowConfig.getWindow().equals(rightDataflowConfig.getWindow())) {
                        throw new Exception(String.format("Please set the same window size for joining dataflows "
                            + "`%s` and `%s`", dataflowConfig.getName(), rightDataflowConfig.getName()));
                    }

                    if (!collectionContext.containsKey(rightDataflowName )) {

                        composePipeline(pipeline, collectionContext, rightDataflowName , endpoints, dataflowParser);

                    }

                    PCollection<Row> right = collectionContext.get(rightDataflowName);

                    PCollectionTuple tableAndRight = PCollectionTuple.of(new TupleTag<>(dataflowConfig.getSource()),
                            table).and(new TupleTag<>(rightDataflowName ), right);

                    table = tableAndRight.apply(
                        String.format("Join dataflows `%s` and `%s`", dataflowConfig.getSource(), rightDataflowName),
                        SqlTransform.query(
                            String.format(queryWithJoinTemplate, getSelect(dataflowConfig.getSelect()),
                                dataflowConfig.getSource(), rightDataflowName, dataflowConfig.getJoin().getWhere())
                            + getFilter(dataflowConfig.getFilter()) + getGroupBy(dataflowConfig.getGroupBy()))
                    );

                } else {
                    table = table.apply(String.format("Transform `%s` to `%s`", dataflowConfig.getSource(),
                        dataflowName), SqlTransform.query(String.format(simpleQueryTemplate,
                        getSelect(dataflowConfig.getSelect())) + getFilter(dataflowConfig.getFilter())
                        + getGroupBy(dataflowConfig.getGroupBy())));
                }

                collectionContext.put(dataflowName, table);

                if (dataflowConfig.getSink() != null && dataflowConfig.getSink() != "") {

                    String sinkName = dataflowConfig.getSink();

                    endpoints.get(sinkName).writeRows(table);

                }

                addSuccessor(pipeline, collectionContext, dataflowName, endpoints, dataflowParser);

            } else {
                throw new Exception(String.format("Invalidate configuration: please specify the source for "
                    + "the dataflow `%s`", dataflowName));
            }

        } else {
            throw new Exception(String.format("Invalidate configuration: not found dataflow `%s`", dataflowName));
        }


    }

    public static void run(DataflowProcessorOptions options) throws Exception {

        Pipeline pipeline = Pipeline.create(options);

        EndpointParser endpointParser = new EndpointParser(options.getEndpointConfigFilePath());

        DataflowParser dataflowParser = new DataflowParser(options.getDataflowConfigFilePath());

        HashMap<String, PCollection<Row>> collectionContext = new HashMap<String, PCollection<Row>> ();

        composePipeline(pipeline, collectionContext, dataflowParser.getFirstDataflow(),
            endpointParser.getEndpoints(), dataflowParser);

        pipeline.run().waitUntilFinish();

    }

    public static void main(String[] args) throws Exception {

        DataflowProcessorOptions options =
            PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowProcessorOptions.class);

        run(options);

    }

}
