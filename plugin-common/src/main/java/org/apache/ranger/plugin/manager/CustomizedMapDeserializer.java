package org.apache.ranger.plugin.manager;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


/**
 * Created by gautam on 06/01/15.
 */
public class CustomizedMapDeserializer extends JsonDeserializer<Map<Object, Object>> {

        @Override
        public Map<Object, Object> deserialize(JsonParser jp, DeserializationContext arg1) throws IOException,
                JsonProcessingException {
                ObjectMapper mapper = (ObjectMapper) jp.getCodec();
                if (jp.getCurrentToken().equals(JsonToken.START_OBJECT)) {
                        return mapper.readValue(jp, new TypeReference<Map<Object, Object>>() { });
                } else {
                        // consume this stream
                        mapper.readTree(jp);
                        return new HashMap<Object, Object>();
                }
        }
}
