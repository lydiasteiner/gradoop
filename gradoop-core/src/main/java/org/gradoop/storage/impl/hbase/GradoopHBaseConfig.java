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

package org.gradoop.storage.impl.hbase;

import org.apache.commons.lang.StringUtils;
import org.gradoop.config.GradoopConfig;
import org.gradoop.config.GradoopStoreConfig;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.EdgePojoFactory;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.GraphHeadPojoFactory;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.pojo.VertexPojoFactory;
import org.gradoop.storage.api.EdgeHandler;
import org.gradoop.storage.api.GraphHeadHandler;
import org.gradoop.storage.api.VertexHandler;
import org.gradoop.util.GConstants;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Configuration class for using HBase with Gradoop.
 *
 * @param <G>   EPGM graph head type
 * @param <V>   EPGM vertex type
 * @param <E>   EPGM edge type
 */
public class GradoopHBaseConfig<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge>
  extends GradoopStoreConfig<G, V, E,
  HBaseGraphHead,
  HBaseVertex<E>,
  HBaseEdge<V>> {

  /**
   * Graph table name.
   */
  private final String graphTableName;
  /**
   * Vertex table name.
   */
  private final String vertexTableName;

  /**
   * Edge table name.
   */
  private final String edgeTableName;

  /**
   * Creates a new Configuration.
   *
   * @param graphHeadHandler            graph head handler
   * @param vertexHandler               vertex handler
   * @param edgeHandler                 edge handler
   * @param graphTableName              graph table name
   * @param vertexTableName             vertex table name
   * @param edgeTableName               edge table name
   */
  private GradoopHBaseConfig(
    GraphHeadHandler<G> graphHeadHandler,
    VertexHandler<V, E> vertexHandler,
    EdgeHandler<V, E> edgeHandler,
    String graphTableName,
    String vertexTableName,
    String edgeTableName) {
    super(graphHeadHandler,
      vertexHandler,
      edgeHandler,
      new HBaseGraphHeadFactory<G>(),
      new HBaseVertexFactory<V, E>(),
      new HBaseEdgeFactory<V, E>());
    checkArgument(!StringUtils.isEmpty(graphTableName),
      "Graph table name was null or empty");
    checkArgument(!StringUtils.isEmpty(vertexTableName),
      "Vertex table name was null or empty");
    checkArgument(!StringUtils.isEmpty(edgeTableName),
      "Edge table name was null or empty");

    this.graphTableName = graphTableName;
    this.vertexTableName = vertexTableName;
    this.edgeTableName = edgeTableName;
  }

  /**
   * Creates a new Configuration.
   *
   * @param config          Gradoop configuration
   * @param graphTableName  graph table name
   * @param vertexTableName vertex table name
   * @param edgeTableName   edge table name
   */
  private GradoopHBaseConfig(GradoopConfig<G, V, E> config,
    String vertexTableName,
    String edgeTableName,
    String graphTableName) {
    this(config.getGraphHeadHandler(),
      config.getVertexHandler(),
      config.getEdgeHandler(),
      graphTableName,
      vertexTableName,
      edgeTableName);
  }

  /**
   * Creates a default Configuration using POJO handlers for vertices, edges
   * and graph heads and default table names.
   *
   * @return Default Gradoop HBase configuration.
   */
  public static GradoopHBaseConfig<
    GraphHeadPojo,
    VertexPojo,
    EdgePojo>
  getDefaultConfig() {
    GraphHeadHandler<GraphHeadPojo> graphHeadHandler =
      new HBaseGraphHeadHandler<>(new GraphHeadPojoFactory());
    VertexHandler<VertexPojo, EdgePojo> vertexHandler =
      new HBaseVertexHandler<>(new VertexPojoFactory());
    EdgeHandler<VertexPojo, EdgePojo> edgeHandler =
      new HBaseEdgeHandler<>(new EdgePojoFactory());

    return new GradoopHBaseConfig<>(
      graphHeadHandler,
      vertexHandler,
      edgeHandler,
      GConstants.DEFAULT_TABLE_GRAPHS,
      GConstants.DEFAULT_TABLE_VERTICES,
      GConstants.DEFAULT_TABLE_EDGES);
  }

  /**
   * Creates a Gradoop HBase configuration based on the given arguments.
   *
   * @param gradoopConfig   Gradoop configuration
   * @param graphTableName  graph table name
   * @param vertexTableName vertex table name
   * @param edgeTableName   edge table name
   * @param <G>             EPGM graph head type
   * @param <V>             EPGM vertex type
   * @param <E>             EPGM edge type
   *
   * @return Gradoop HBase configuration
   */
  public static <
    G extends EPGMGraphHead,
    V extends EPGMVertex,
    E extends EPGMEdge>
  GradoopHBaseConfig<G, V, E> createConfig(
    GradoopConfig<G, V, E> gradoopConfig,
    String vertexTableName,
    String edgeTableName,
    String graphTableName) {
    return new GradoopHBaseConfig<>(gradoopConfig,
      graphTableName,
      vertexTableName,
      edgeTableName);
  }

  public String getVertexTableName() {
    return vertexTableName;
  }

  public String getEdgeTableName() {
    return edgeTableName;
  }

  public String getGraphTableName() {
    return graphTableName;
  }
}
