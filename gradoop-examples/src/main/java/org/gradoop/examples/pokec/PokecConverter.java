package org.gradoop.examples.pokec;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.examples.pokec.functions.PokecToImportEdge;
import org.gradoop.examples.pokec.functions.PokecToImportVertex;
import org.gradoop.io.api.DataSink;
import org.gradoop.io.impl.graph.GraphDataSource;
import org.gradoop.io.impl.graph.tuples.ImportEdge;
import org.gradoop.io.impl.graph.tuples.ImportVertex;
import org.gradoop.io.impl.json.JSONDataSink;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.properties.PropertyList;
import org.gradoop.util.GradoopFlinkConfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Transforms data from pokec (http://snap.stanford.edu/data/soc-pokec.html)
 * into JSON file format.
 */
public class PokecConverter extends AbstractRunner implements
  ProgramDescription {


  public static void main(String[] args) {

    DataSet<String> vertexStrings = getExecutionEnvironment().readTextFile
      ("gradoop-examples/src/main/resources/pokec/soc-pokec-profiles.txt");
    DataSet<ImportVertex<Long>> importVertices = vertexStrings.map(
      new PokecToImportVertex());

    DataSet<String> edgeStrings = getExecutionEnvironment().readTextFile(
      "gradoop-examples/src/main/resources/pokec/soc-pokec-relationships.txt");

    DataSet<Tuple2<Long, String>> edgeStringsWithIds = DataSetUtils
      .zipWithUniqueId(edgeStrings);

    DataSet<ImportEdge<Long>> importEdges = edgeStringsWithIds.map(
      new PokecToImportEdge());

    GradoopFlinkConfig<GraphHeadPojo, VertexPojo, EdgePojo> conf =
      GradoopFlinkConfig.createDefaultConfig(getExecutionEnvironment());

    GraphDataSource<GraphHeadPojo, VertexPojo, EdgePojo, Long> dataSource =
      new
      GraphDataSource<>(
      importVertices, importEdges, conf);

    try {
      dataSource.getLogicalGraph().writeTo(
        new JSONDataSink<>(
        "gradoop-examples/src/main/resources/pokec/output/graphHeads.json",
        "gradoop-examples/src/main/resources/pokec/output/vertices.json",
        "gradoop-examples/src/main/resources/pokec/output/edges.json",
        conf
      ));
      getExecutionEnvironment().execute();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public String getDescription() {
    return PokecConverter.class.getName();
  }
}
