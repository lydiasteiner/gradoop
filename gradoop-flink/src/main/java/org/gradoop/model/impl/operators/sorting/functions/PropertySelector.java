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

package org.gradoop.model.impl.operators.sorting.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.properties.PropertyValue;

/**
 * Select the value of a specific property.
 *
 * @param <EL> element type
 */
@FunctionAnnotation.ForwardedFields("id->*")
public class PropertySelector<EL extends EPGMElement>
  implements KeySelector<EL, PropertyValue> {


  private String propertyKey;

  public PropertySelector(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public PropertyValue getKey(EL element) throws Exception {
    System.out.println(element.getPropertyValue(propertyKey));
    return element.getPropertyValue(propertyKey);
  }
}
