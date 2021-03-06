/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.operators.aggregation;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.functions.AggregateFunction;
import org.gradoop.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.epgm.PropertySetterBroadcast;
import org.gradoop.model.impl.properties.PropertyValue;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Takes a logical graph and a user defined aggregate function as input. The
 * aggregate function is applied on the logical graph and the resulting
 * aggregate is stored as an additional property at the result graph.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 * @param <N> output type of aggregate function
 */
public class Aggregation<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge,
  N extends Number>
  implements UnaryGraphToGraphOperator<G, V, E> {

  /**
   * Used to store aggregate result.
   */
  private final String aggregatePropertyKey;

  /**
   * User-defined aggregate function which is applied on a single logical graph.
   */
  private final AggregateFunction<G, V, E> aggregationFunction;

  /**
   * Creates new aggregation.
   *
   * @param aggregatePropertyKey property key to store result of {@code
   *                             aggregationFunction}
   * @param aggregationFunction  user defined aggregation function which gets
   *                             called on the input graph
   */
  public Aggregation(final String aggregatePropertyKey,
    final AggregateFunction<G, V, E> aggregationFunction) {
    this.aggregatePropertyKey = checkNotNull(aggregatePropertyKey);
    this.aggregationFunction = checkNotNull(aggregationFunction);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> execute(LogicalGraph<G, V, E> graph) {

    DataSet<PropertyValue> aggregateValue = aggregationFunction.execute(graph);

    DataSet<G> graphHead = graph.getGraphHead()
      .map(new PropertySetterBroadcast<G>(aggregatePropertyKey))
      .withBroadcastSet(aggregateValue, PropertySetterBroadcast.VALUE);

    return LogicalGraph.fromDataSets(
        graphHead,
        graph.getVertices(),
        graph.getEdges(),
        graph.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Aggregation.class.getName();
  }
}
