package org.gradoop.examples.pokec.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.io.impl.graph.tuples.ImportVertex;
import org.gradoop.model.impl.properties.Property;
import org.gradoop.model.impl.properties.PropertyList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Transforms one line from the pokec profiles to an import vertex.
 */
public class PokecToImportVertex
  implements MapFunction<String, ImportVertex<Long>> {

  Pattern TOKEN_SEPARATOR = Pattern.compile("\\t");

  private static final String[] COLUMNS = {
    "user_id", "public", "completion_percentage", "gender", "region",
    "last_login", "registration", "AGE", "body", "I_am_working_in_field",
    "spoken_languages", "hobbies", "I_most_enjoy_good_food", "pets",
    "body_type", "my_eyesight", "eye_color", "hair_color", "hair_type",
    "completed_level_of_education", "favourite_color", "relation_to_smoking",
    "relation_to_alcohol", "sign_in_zodiac", "on_pokec_i_am_looking_for",
    "love_is_for_me", "relation_to_casual_sex", "my_partner_should_be",
    "marital_status", "children", "relation_to_children", "I_like_movies",
    "I_like_watching_movie", "I_like_music", "I_mostly_like_listening_to_music",
    "the_idea_of_good_evening", "I_like_specialties_from_kitchen", "fun",
    "I_am_going_to_concerts", "my_active_sports", "my_passive_sports",
    "profession", "I_like_books", "life_style", "music", "cars", "politics",
    "relationships", "art_culture", "hobbies_interests", "science_technologies",
    "computers_internet", "education", "sport", "movies", "travelling",
    "health", "companies_brands", "more"
  };

  @Override
  public ImportVertex<Long> map(String line) throws Exception {

    String[] fields = TOKEN_SEPARATOR.split(line);

    Long id = Long.parseLong(fields[0]);

    String label = "profile";

    PropertyList properties = PropertyList.create();

    for(int i=1;i<fields.length;i++) {

      String value = fields[i];
      if(value.equals("null")) {
        break;
      }
      if(value.length() > 50) {
        break;
      }
      Property property = Property.create(COLUMNS[i], value);
      properties.set(property);
    }





    return new ImportVertex<>(id, label, properties);
  }
}
