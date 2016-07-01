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

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.impl.properties.PropertyValue;

/**
 * Select the value of a specific property.
 *
 * @param <EL> element type
 */
public class PropertySelector<EL extends EPGMElement>
  implements KeySelector<EL, PropertyValue> {

  /**
   * Key of the property, that is to be selected.
   */
  private String propertyKey;

  /**
   * Constructor
   *
   * @param propertyKey key of property that is to be selected
   */
  public PropertySelector(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  /**
   * Return the value
   * @param element element type
   * @return property value
   */
  @Override
  public PropertyValue getKey(EL element) {
    return element.getPropertyValue(propertyKey);
  }
}
