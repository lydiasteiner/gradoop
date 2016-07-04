package org.gradoop.examples.pokec.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.io.impl.graph.tuples.ImportEdge;
import org.gradoop.io.impl.graph.tuples.ImportVertex;
import org.gradoop.model.impl.properties.Property;
import org.gradoop.model.impl.properties.PropertyList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Transforms one line from the pokec relationships to an import edge.
 */
public class PokecToImportEdge
  implements MapFunction<Tuple2<Long, String>, ImportEdge<Long>> {


  @Override
  public ImportEdge<Long> map(Tuple2<Long, String> edgeTuple) throws
    Exception {

    Long id = edgeTuple.f0;
    String[] fields =edgeTuple.f1.split("\\s+");
    Long source = Long.parseLong(fields[0]);
    Long target = Long.parseLong(fields[1]);
    String label = "relationship";

    PropertyList properties = PropertyList.createWithCapacity(0);

    return new ImportEdge<>(id, source, target, label, properties);
  }
}
