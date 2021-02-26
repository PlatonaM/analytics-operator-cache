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


import org.infai.ses.senergy.operators.Config;
import org.infai.ses.senergy.operators.Stream;
import org.infai.ses.senergy.utils.ConfigProvider;

public class Operator {

    public static void main(String[] args) throws Exception {
        InputParser parser = new InputParser();
        Config config = ConfigProvider.getConfig();
        parser.parse(config.getInputTopicsConfigs());
        Cache cache = new Cache(
                config.getConfigValue("time_input", null),
                config.getConfigValue("batch_pos_input", null),
                config.getConfigValue("batch_pos_start", null),
                config.getConfigValue("batch_pos_end", null),
                Long.parseLong(config.getConfigValue("time_window", "0")),
                Boolean.parseBoolean(config.getConfigValue("compress_output", "false")),
                parser.getInputs()
        );
        Stream stream  = new Stream();
        stream.start(cache);
    }
}
