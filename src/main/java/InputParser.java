/*
 * Copyright 2021 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.infai.ses.senergy.models.InputTopicModel;
import org.infai.ses.senergy.models.MappingModel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class InputParser {

    private final Map<String, String> inputMap = new HashMap<>();
    private final List<Map<String, Object>> inputSources = new ArrayList<>();

    public void parse(List<InputTopicModel> inputTopics) {
        String sep = "\\.";
        for (InputTopicModel inputTopic : inputTopics) {
            List<MappingModel> mappings = inputTopic.getMappings();
            for (MappingModel mapping : mappings) {
                String[] source = mapping.getSource().split(sep);
                inputMap.put(mapping.getDest(), source[source.length - 1]);
            }
            Map<String, Object> inputSource = new HashMap<>();
            inputSource.put("id", inputTopic.getFilterValue());
            inputSource.put("name", inputTopic.getName());
            inputSources.add(inputSource);
        }
    }

    public Map<String, String> getInputs() {
        return inputMap;
    }

    public List<Map<String, Object>> getInputSources() {
        return inputSources;
    }
}
