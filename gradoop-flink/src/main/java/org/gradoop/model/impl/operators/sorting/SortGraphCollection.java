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
package org.gradoop.model.impl.operators.sorting;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.operators.sorting.functions.PropertySelector;
import org.gradoop.util.Order;

/**
 * Sorts the graphs in a graph collection by the value of a specified
 * property key.
 *
 * @param <G> graph head type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class SortGraphCollection<G extends EPGMGraphHead, V extends
  EPGMVertex, E extends EPGMEdge> implements
  UnaryCollectionToCollectionOperator<G, V, E> {
  /**
   * Key that specifies the property used to sort.
   */
  private String propertyKey;
  /**
   * Order in which to sort: Ascending or Descending.
   */
  private org.apache.flink.api.common.operators.Order order;

  /**
   * Constructor
   *
   * @param propertyKey key that specifies the property used to sort
   * @param order ascending or descending order
   */
  public SortGraphCollection(String propertyKey, Order order) {
    this.propertyKey = propertyKey;

    if (order.equals(Order.ASCENDING)) {
      this.order = org.apache.flink.api.common.operators.Order.ASCENDING;
    } else if (order.equals(Order.DESCENDING)) {
      this.order = org.apache.flink.api.common.operators.Order.DESCENDING;
    } else {
      this.order = null;
    }
  }

  /**
   * {@inheritDoc}
   *
   * @param collection input collection
   * @return
   */
  @Override
  public GraphCollection<G, V, E> execute(GraphCollection<G, V, E> collection) {
    DataSet<G> newGraphHeads = collection.getGraphHeads();
    newGraphHeads.partitionByRange(new PropertySelector<G>(propertyKey));
    newGraphHeads = newGraphHeads.sortPartition(
      new PropertySelector<G>(propertyKey), order);
    return GraphCollection.fromDataSets(newGraphHeads, collection.getVertices(),
      collection.getEdges(), collection.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return SortGraphCollection.class.getName();
  }
}
