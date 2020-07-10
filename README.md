# Tesey DataFlow 

Tesey DataFlow lets you process dataflows as in batch and as well in streaming modes 
in any Apache Beam's supported execution engines including Apache Spark, Apache Flink, Apache Samza, etc.

## Installation

Clone the repository, and install the package with

```
mvn clean install
```

## Usage

1. Describe <a href="#dataflow.io/EndpointsSpecification">endpoints</a> used in processing data in endpoints.yaml similar to following:
```yaml
endpoints:
  - name: authorization
    type: kafka
    schemaPath: avro/authorization.avsc
    format: avro
    options:
    - name: topic
      value: authorization
    - name: bootstrapServers
      value: kafka-cp-kafka-headless:9092
  - name: transaction
    type: kafka
    schemaPath: avro/transaction.avsc
    format: avro
    options:
    - name: topic
      value: transaction
    - name: bootstrapServers
      value: kafka-cp-kafka-headless:9092
  - name: summary
    type: kafka
    schemaPath: avro/summary.avsc
    format: avro
    options:
    - name: topic
      value: summary
    - name: bootstrapServers
      value: kafka-cp-kafka-headless:9092
  - name: customer
    type: kafka
    schemaPath: avro/customer.avsc
    format: avro
    options:
    - name: topic
      value: customer
    - name: bootstrapServers
      value: kafka-cp-kafka-headless:9092
  - name: report
    type: kafka
    schemaPath: avro/report.avsc
    format: avro
    options:
    - name: topic
      value: report
    - name: bootstrapServers
      value: kafka-cp-kafka-headless:9092
  - name: groupedAuthorizationsByCnum
    type: kafka
    schemaPath: avro/groupedAuthorizationsByCnum.avsc
    format: avro
    options:
    - name: topic
      value: groupedAuthorizationsByCnum
    - name: bootstrapServers
      value: kafka-cp-kafka-headless:9092
```

2. Describe <a href="#dataflow.io/DataflowsSpecification">dataflows</a> that should be processed in endpoints.yaml similar to following:
```yaml
dataflows:
  - name: authorizationStream
    source: authorization
    isFirst: true
    window: 60000
  - name: transactionStream
    source: transaction
    window: 60000
  - name: summaryStream
    source: authorizationStream
    select: "authorizationStream.operationId, authorizationStream.cnum, authorizationStream.amount, authorizationStream.currency, authorizationStream.authTime, transaction.entryId, transaction.entryTime"
    window: 60000
    join:
      dataflow: transactionStream
      where: "authorizationStream.operationId = transaction.operationId"
    sink: summary
  - name: customerStream
    source: customer
    window: 60000
  - name: reportStream
    source: summaryStream
    select: "summaryStream.operationId, summaryStream.cnum, customerStream.firstName, customerStream.lastName, summaryStream.amount, summaryStream.currency, summaryStream.authTime, summaryStream.entryId, summaryStream.entryTime"
    join:
      dataflow: customerStream
      where: "summaryStream.cnum = customerStream.cnum"
    sink: report
  - name: groupedAuthorizationsByCnumStream
    source: summaryStream
    select: "authorizationStream.cnum, SUM(authorizationStream.amount) AS total_amount"
    groupBy: "authorizationStream.cnum"
    sink: groupedAuthorizationsByCnum
```

3. Submit the application like the following specified the pathes to endpoints.yaml and application.yaml 
in options <code>endpointConfigFilePath</code> and <code>dataflowConfigFilePath</code> respectively: 

### Submit Spark Application
```shell script
spark-submit \
--class org.tesey.dataflow.DataflowProcessor \
--master yarn \
--deploy-mode cluster \
target/tesey-dataflow-1.0-SNAPSHOT.jar \
--runner=SparkRunner \
--streaming=true \
--endpointConfigFilePath=configs/endpoints.yaml \
--dataflowConfigFilePath=configs/application.yaml
```

### Submit Flink Application
```shell script
./bin/flink run \
-m yarn-cluster \
-c org.tesey.dataflow.DataflowProcessor target/tesey-dataflow-1.0-SNAPSHOT.jar \
--runner=FlinkRunner \
--streaming=true \
--endpointConfigFilePath=configs/endpoints.yaml \
--dataflowConfigFilePath=configs/application.yaml
```

<h2 id="dataflow.io/EndpointsSpecification">Endpoints specification
</h2>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>name</code></br>
string</td>
<td>
The name used to identify endpoint
</td>
</tr>
<tr>
<td>
<code>type</code></br>
string</td>
<td>
The type of endpoint, now is supported types are <code>kafka</code> and <code>file</code>
</td>
</tr>
<tr>
<td>
<code>schemaPath</code></br>
string</td>
<td>
The path to Avro schema that corresponds with the structure of ingesting/exporting records
</td>
</tr>
<tr>
<td>
<code>format</code></br>
string</td>
<td>
The format of ingesting/exporting data. currently supported formats are <code>avro</code> and <code>parquet</code> 
</td>
</tr>
<tr>
<td>
<code>options</code>
<td>
The set of options depends on the <a href="#dataflow.io/EndpointType">endpoint type</a> 
</td>
</tr>
</tbody>
</table>


<h3 id="dataflow.io/EndpointType">EndpointType
</h3>
The DataflowAggregator currently supported the following endpoint types:

* <code>kafka</code> - the endpoint type used to read/write messages in Apache Kafka topics
* <code>file</code> - the endpoint type used to read/write files to HDFS or to object storage like S3, GS, etc.

### Kafka endpoint options
<table>
<thead>
<tr>
<th>Name</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>topic</code></br>
string</td>
<td>
The name of Kafka topic
</td>
</tr>
<tr>
<td>
<code>bootstrapServers</code></br>
string</td>
<td>
A comma-separated list of host and port pairs that are the addresses of the Kafka brokers
</td>
</tbody>
</table>

### File endpoint options
<table>
<thead>
<tr>
<th>Name</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>pathToDataset</code></br>
string</td>
<td>
The path of ingesting/exporting files
</td>
</tr>
</tbody>
</table>

<h2 id="dataflow.io/DataflowsSpecification">Dataflows specification
</h2>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>name</code></br>
string</td>
<td>
The name used to identify dataflow
</td>
</tr>
<tr>
<td>
<code>isFirst</code></br>
boolean</td>
<td>
The flag telling the dataflow should be processed firstly
</td>
</tr>
<tr>
<td>
<code>source</code></br>
string</td>
<td>
The name of source / dataflow used to read data from
</td>
</tr>
<tr>
<td>
<code>select</code></br>
string</td>
<td>
A comma-separated list of selected fields
</td>
</tr>
<tr>
<td>
<code>filter</code></br>
string</td>
<td>
The filter predicate
</td>
</tr>
<tr>
<td>
<code>sink</code></br>
string</td>
<td>
The name of endpoint that should be used as sink to write records to
</td>
</tr>
<tr>
<td>
<code>groupBy</code></br>
string</td>
<td>
A comma-separated list of fields which is used for grouping rows on
</td>
</tr>
<tr>
<tr>
<td>
<code>window</code></br>
integer</td>
<td>
Window size in milliseconds
</td>
</tr>
<tr>
<td>
<code>join</code></br>
</td>
<td>
<br/>
<br/>
<table>
<tr>
<td>
<code>dataflow</code></br>
<em>
string
</em>
</td>
<td>
<p>The name of dataflow that should be joined with the dataflow described above</p>
</td>
</tr>
<tr>
<td>
<code>where</code></br>
<em>
string
</em>
</td>
<td>
<p>The join predicate</p>
</td>
</tr>
</table>
</td>
</tr>
</tbody>
</table>
