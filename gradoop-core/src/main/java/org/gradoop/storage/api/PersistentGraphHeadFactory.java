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

package org.gradoop.storage.api;

import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.impl.id.GradoopIdSet;

import java.io.Serializable;

/**
 * Base interface for creating persistent graph data from transient graph data.
 *
 * @param <G>   EPGM graph head type
 * @param <PG>  Persistent graph head type
 */
public interface PersistentGraphHeadFactory
  <G extends EPGMGraphHead, PG extends PersistentGraphHead>
  extends Serializable {

  /**
   * Creates graph data based on the given parameters.
   *
   * @param inputGraphData input graph data
   * @param vertices       vertices contained in that graph
   * @param edges          edges contained in that graph
   * @return graph data
   */
  PG createGraphHead(
    G inputGraphData, GradoopIdSet vertices, GradoopIdSet edges);
}
