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

package org.gradoop.model.impl.algorithms.fsm.gspan.miners.filterrefine.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.model.impl.tuples.WithCount;
import org.gradoop.model.impl.algorithms.fsm.gspan.miners.filterrefine.tuples.RefinementMessage;

/**
 * refinementMessage => (subgraph, frequency)
 */
public class CompressedSubgraphWithCount
  implements MapFunction<RefinementMessage, WithCount<CompressedDFSCode>> {

  @Override
  public WithCount<CompressedDFSCode> map(
    RefinementMessage message) throws Exception {

    return new WithCount<>(message.getSubgraph(), message.getSupport());
  }
}
