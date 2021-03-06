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

package org.gradoop.model.impl.pojo;

import com.google.common.base.Preconditions;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.properties.PropertyList;
import org.gradoop.util.GConstants;
import org.gradoop.model.api.EPGMVertexFactory;

/**
 * Factory for creating vertex POJOs.
 */
public class VertexPojoFactory implements EPGMVertexFactory<VertexPojo> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexPojo createVertex() {
    return initVertex(GradoopId.get());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexPojo initVertex(final GradoopId vertexID) {
    return initVertex(vertexID, GConstants.DEFAULT_VERTEX_LABEL, null, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexPojo createVertex(String label) {
    return initVertex(GradoopId.get(), label);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexPojo initVertex(final GradoopId vertexID, final String label) {
    return initVertex(vertexID, label, null, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexPojo createVertex(String label, PropertyList properties) {
    return initVertex(GradoopId.get(), label, properties);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexPojo initVertex(final GradoopId vertexID, final String label,
    PropertyList properties) {
    return initVertex(vertexID, label, properties, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexPojo createVertex(String label, GradoopIdSet graphIds) {
    return initVertex(GradoopId.get(), label, graphIds);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexPojo initVertex(final GradoopId vertexID, final String label,
    final GradoopIdSet graphs) {
    return initVertex(vertexID, label, null, graphs);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexPojo createVertex(String label, PropertyList properties,
    GradoopIdSet graphIds) {
    return initVertex(GradoopId.get(), label, properties, graphIds);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexPojo initVertex(final GradoopId id, final String label,
    final PropertyList properties, final GradoopIdSet graphs) {
    Preconditions.checkNotNull(id, "Identifier was null");
    Preconditions.checkNotNull(label, "Label was null");
    return new VertexPojo(id, label, properties, graphs);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<VertexPojo> getType() {
    return VertexPojo.class;
  }
}
