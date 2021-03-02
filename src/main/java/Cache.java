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


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.infai.ses.senergy.exceptions.NoValueException;
import org.infai.ses.senergy.operators.BaseOperator;
import org.infai.ses.senergy.operators.Message;
import org.infai.ses.senergy.util.DateParser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.zip.GZIPOutputStream;


public class Cache extends BaseOperator {

    private final String timeInput;
    private final String batchPosInput;
    private final String batchPosStart;
    private final String batchPosEnd;
    private String batchPos;
    private final long timeWindow;
    private final boolean compressOutput;
    private long currentTimestamp;
    private long startTimestamp = -1;
    private final Set<String> inputSources;
    private final Map<String, String> inputMap;
    private final List<Map<String, Object>> messages = new ArrayList<>();
    private final List<Map<String, Object>> messages2 = new ArrayList<>();
    private final String cacheOutput = "data";
    private final String metaOutput = "meta_data";

    public Cache(String timeInput, String batchPosInput, String batchPosStart, String batchPosEnd, long timeWindow, boolean compressOutput, Set<String> inputSources, Map<String, String> inputMap) throws Exception {
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
        this.inputSources = inputSources;
        this.inputMap = inputMap;
    }

    private String compress(String str) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream);
        gzipOutputStream.write(str.getBytes());
        gzipOutputStream.close();
        byte[] bytes = outputStream.toByteArray();
        return Base64.getEncoder().withoutPadding().encodeToString(bytes);
    }

    private String toJSON(List<Map<String, Object>> messages) {
        Gson gson = new GsonBuilder().serializeNulls().create();
        Type collectionType = new TypeToken<List<Map<String, Object>>>(){}.getType();
        return gson.toJson(messages, collectionType);
    }

    private void outputMessage(Message message, List<Map<String, Object>> messages) {
        if (compressOutput) {
            try {
                message.output(cacheOutput, compress(toJSON(messages)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            message.output(cacheOutput, toJSON(messages));
        }
        message.output(metaOutput, toJSON(inputSources));
    }

    @Override
    public void run(Message message) {
        Map<String, Object> msg = new HashMap<>();
        for (Map.Entry<String, String> entry : inputMap.entrySet()) {
            try {
                Object valueObj = message.getInput(entry.getKey()).getValue(Object.class);
                if (!entry.getKey().equals(batchPosInput)) {
                    msg.put(entry.getValue(), valueObj);
                }
                if (entry.getKey().equals(timeInput)) {
                    currentTimestamp = DateParser.parseDateMills((String) valueObj);
                    if (startTimestamp < 0) {
                        startTimestamp = currentTimestamp;
                    }
                }
                if (entry.getKey().equals(batchPosInput)) {
                    batchPos = (String) valueObj;
                    if (batchPos.equals(batchPosStart)) {
                        System.out.println("received start of batch data");
                    }
                    if (batchPos.equals(batchPosEnd)) {
                        System.out.println("received end of batch data");
                    }
                }
            } catch (NoValueException e) {
                if (!entry.getKey().equals(batchPosInput)) {
                    msg.put(entry.getValue(), null);
                }
            }
        }
        message.output(cacheOutput, null);
        if (batchPos.equals(batchPosEnd)) {
            messages.add(msg);
            if (!messages2.isEmpty()) {
//                System.out.println("send window: " + messages2.size() + " and remainders: " + messages.size());
                messages2.addAll(messages);
                outputMessage(message, messages2);
                messages2.clear();
            } else {
//                System.out.println("send remainders: " + messages.size());
                outputMessage(message, messages);
            }
            messages.clear();
        } else {
            if (timeWindow > 0) {
                if (currentTimestamp - startTimestamp >= timeWindow) {
                    startTimestamp = currentTimestamp;
                    if (!messages2.isEmpty()) {
//                        System.out.println("send window: " + messages2.size());
                        outputMessage(message, messages2);
                        messages2.clear();
                    }
//                    System.out.println("store window: " + messages.size());
                    messages2.addAll(messages);
                    messages.clear();
                }
            }
            messages.add(msg);
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
