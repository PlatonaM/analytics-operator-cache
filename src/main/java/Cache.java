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


import com.google.gson.reflect.TypeToken;
import org.infai.ses.platonam.util.Compression;
import org.infai.ses.platonam.util.Json;
import org.infai.ses.senergy.exceptions.NoValueException;
import org.infai.ses.senergy.operators.BaseOperator;
import org.infai.ses.senergy.operators.Message;
import org.infai.ses.senergy.util.DateParser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

import static org.infai.ses.platonam.util.Logger.getLogger;


public class Cache extends BaseOperator {

    private static final Logger logger = getLogger(Cache.class.getName());
    private final String timeInput;
    private final String batchPosInput;
    private final String batchPosStart;
    private final String batchPosEnd;
    private final long timeWindow;
    private final boolean compressOutput;
    private final Map<String, String> inputMap;
    private final List<Map<String, Object>> messages = new ArrayList<>();
    private final List<Map<String, Object>> messages2 = new ArrayList<>();
    private final Set<String> nestedMapInputs;
    private String batchPos;
    private long currentTimestamp;
    private String currentTimestampRaw = null;
    private long startTimestamp = -1;

    public Cache(String timeInput, String batchPosInput, String batchPosStart, String batchPosEnd, long timeWindow, boolean compressOutput, Map<String, String> inputMap, String nestedMapInputs) throws Exception {
        if (timeInput == null || timeInput.isBlank()) {
            throw new Exception("invalid time_input: " + timeInput);
        }
        if (batchPosInput == null || batchPosInput.isBlank()) {
            throw new Exception("invalid batch_pos_input: " + batchPosInput);
        }
        if (batchPosStart == null || batchPosStart.isBlank()) {
            throw new Exception("invalid batch_pos_start: " + batchPosStart);
        }
        if (batchPosEnd == null || batchPosEnd.isBlank()) {
            throw new Exception("invalid batch_pos_end: " + batchPosEnd);
        }
        this.timeInput = timeInput;
        this.batchPosInput = batchPosInput;
        this.batchPosStart = batchPosStart;
        this.batchPosEnd = batchPosEnd;
        this.timeWindow = timeWindow * 1000;
        this.compressOutput = compressOutput;
        this.inputMap = inputMap;
        this.nestedMapInputs = nestedMapInputs != null ? Json.fromString(nestedMapInputs, new TypeToken<>() {
        }) : Collections.emptySet();
    }

    private void outputMessage(Message message, List<Map<String, Object>> messages) throws IOException {
        if (compressOutput) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            Json.toStream(new TypeToken<Map<String, Object>>() {
            }.getType(), messages, Compression.compress(outputStream));
            outputStream.close();
            message.output("cached_data", outputStream.toString());
        } else {
            message.output("cached_data", Json.toString(new TypeToken<List<Map<String, Object>>>() {
            }.getType(), messages));
        }
        logger.fine("sent window of " + messages.size() + " messages");
    }

    @Override
    public void run(Message message) {
        try {
            Map<String, Object> msg = new HashMap<>();
            for (Map.Entry<String, String> entry : inputMap.entrySet()) {
                try {
                    if (nestedMapInputs.contains(entry.getKey())) {
                        Map<String, Object> nestedMap = Json.fromString(message.getInput(entry.getKey()).getString(), new TypeToken<>() {
                        });
                        msg.putAll(nestedMap);
                    } else if (entry.getKey().equals(batchPosInput)) {
                        batchPos = message.getInput(entry.getKey()).getString();
                    } else if (entry.getKey().equals(timeInput)) {
                        currentTimestampRaw = message.getInput(entry.getKey()).getString();
                        currentTimestamp = DateParser.parseDateMills(currentTimestampRaw);
                        if (startTimestamp < 0) {
                            startTimestamp = currentTimestamp;
                        }
                        msg.put(entry.getValue(), currentTimestampRaw);
                    } else {
                        msg.put(entry.getValue(), message.getInput(entry.getKey()).getValue(Object.class));
                    }
                } catch (NoValueException e) {
                    if (entry.getKey().equals(timeInput)) {
                        throw e;
                    } else if (entry.getKey().equals(batchPosInput)) {
                        batchPos = "";
                    } else {
                        msg.put(entry.getValue(), null);
                    }
                }
            }
            if (batchPos.equals(batchPosStart)) {
                logger.info("received start of batch data with timestamp '" + currentTimestampRaw + "'");
            }
            if (batchPos.equals(batchPosEnd)) {
                logger.info("received end of batch data with timestamp '" + currentTimestampRaw + "'");
                currentTimestampRaw = null;
                messages.add(msg);
                if (!messages2.isEmpty()) {
                    messages2.addAll(messages);
                    outputMessage(message, messages2);
                    messages2.clear();
                } else {
                    outputMessage(message, messages);
                }
                messages.clear();
            } else {
                if (timeWindow > 0) {
                    if (currentTimestamp - startTimestamp >= timeWindow) {
                        startTimestamp = currentTimestamp;
                        if (!messages2.isEmpty()) {
                            outputMessage(message, messages2);
                            messages2.clear();
                        }
                        messages2.addAll(messages);
                        logger.fine("stored window of " + messages.size() + " messages");
                        messages.clear();
                    }
                }
                messages.add(msg);
            }
        } catch (Throwable t) {
            logger.severe("error handling message near timestamp '" + currentTimestampRaw + "':");
            t.printStackTrace();
        }
    }

    @Override
    public Message configMessage(Message message) {
        for (String key : inputMap.keySet()) {
            message.addInput(key);
        }
        return message;
    }
}
