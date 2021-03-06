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

package org.gradoop.model.impl.tuples;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.config.GradoopConfig;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;

import java.util.Set;

/**
 * An encapsulated representation of a logical graph with duplicated elements.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class GraphTransaction
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends Tuple3<G, Set<V>, Set<E>> {

  /**
   * default constructor
   */
  public GraphTransaction() {
  }

  /**
   * valued constructor
   * @param graphHead graph head
   * @param vertices set of vertices
   * @param edges set of edges
   */
  public GraphTransaction(G graphHead, Set<V> vertices, Set<E> edges) {
    setGraphHead(graphHead);
    setVertices(vertices);
    setEdges(edges);
  }

  public G getGraphHead() {
    return this.f0;
  }

  public void setGraphHead(G graphHead) {
    this.f0 = graphHead;
  }

  public Set<V> getVertices() {
    return this.f1;
  }

  public void setVertices(Set<V> vertices) {
    this.f1 = vertices;
  }

  public Set<E> getEdges() {
    return this.f2;
  }

  public void  setEdges(Set<E> edges) {
    this.f2 = edges;
  }

  /**
   * Returns the Flink type information of a graph transaction.
   *
   * @param config Gradoop configuration
   * @param <G> EPGM graph head type
   * @param <V> EPGM vertex type
   * @param <E> EPGM edge type
   * @return type information
   */
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  TypeInformation<GraphTransaction<G, V, E>> getTypeInformation(
    GradoopConfig<G, V, E> config) {

    Set<V> vertices = Sets.newHashSetWithExpectedSize(1);
    vertices.add(config.getVertexFactory().createVertex());

    Set<E> edges = Sets.newHashSetWithExpectedSize(1);
    edges.add(config.getEdgeFactory()
      .createEdge(GradoopId.get(), GradoopId.get()));

    return TypeExtractor.getForObject(
      new GraphTransaction<>(
        config.getGraphHeadFactory().createGraphHead(), vertices, edges));
  }
}
