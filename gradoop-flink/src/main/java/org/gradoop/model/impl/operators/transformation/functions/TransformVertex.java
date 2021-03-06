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

package org.gradoop.model.impl.operators.transformation.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.api.functions.TransformationFunction;
import org.gradoop.util.GConstants;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Transformation map function for vertices.
 *
 * @param <V> EPGM vertex type
 */
@FunctionAnnotation.ForwardedFields("id;graphIds")
public class TransformVertex<V extends EPGMVertex> extends TransformBase<V> {

  /**
   * Factory to init modified vertex.
   */
  private final EPGMVertexFactory<V> vertexFactory;

  /**
   * Constructor
   *
   * @param transformationFunction  vertex modification function
   * @param vertexFactory         vertex factory
   */
  public TransformVertex(TransformationFunction<V> transformationFunction,
    EPGMVertexFactory<V> vertexFactory) {
    super(transformationFunction);
    this.vertexFactory = checkNotNull(vertexFactory);
  }

  @Override
  protected V initFrom(V element) {
    return vertexFactory.initVertex(
      element.getId(), GConstants.DEFAULT_VERTEX_LABEL, element.getGraphIds());
  }
}
