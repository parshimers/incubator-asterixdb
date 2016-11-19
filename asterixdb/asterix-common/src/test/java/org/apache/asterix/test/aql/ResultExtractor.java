/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.test.aql;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.commons.io.IOUtils;

/**
 * extracts results from the response of the QueryServiceServlet.
 * As the response is not necessarily valid JSON, non-JSON content has to be extracted in some cases.
 * The current implementation creates a toomany copies of the data to be usable for larger results.
 */
public class ResultExtractor {

    private static final Logger LOGGER = Logger.getLogger(ResultExtractor.class.getName());

    public static InputStream extract(InputStream resultStream) throws Exception {
        ObjectMapper om = new ObjectMapper();
        JsonNode result = om.readTree(resultStream);

        LOGGER.fine("+++++++\n" + result + "\n+++++++\n");

        String type = "";
        String status = "";
        String results = "";
        String field = "";
        for (Iterator<String> sIter = result.fieldNames(); sIter.hasNext();) {
            field = sIter.next();
            switch (field) {
                case "requestID":
                    break;
                case "signature":
                    break;
                case "status":
                    status = result.get(field).asText();
                    break;
                case "type":
                    type = result.get(field).asText();
                    break;
                case "metrics":
                    LOGGER.fine(result.get(field).asText());
                    break;
                case "errors":
                    JsonNode errors = result.get(field).get(0).get("msg");
                    throw new AsterixException(errors.asText());
                case "results":
                    results = result.get(field).asText();
                    break;
                default:
                    throw new AsterixException(field + "unanticipated field");
            }
        }

        return IOUtils.toInputStream(results);
        //        JSONTokener tokener = new JSONTokeVner(result);
        //        tokener.nextTo('{');
        //        tokener.next('{');
        //        while ((name = getFieldName(tokener)) != null) {
        //            if ("requestID".equals(name) || "signature".equals(name)) {
        //                getStringField(tokener);
        //            } else if ("status".equals(name)) {
        //                status = getStringField(tokener);
        //            } else if ("type".equals(name)) {
        //                type = getStringField(tokener);
        //            } else if ("metrics".equals(name)) {
        //                JSONObject metrics = getObjectField(tokener);
        //                LOGGER.fine(name + ": " + metrics);
        //            } else if ("errors".equals(name)) {
        //                JSONArray errors = getArrayField(tokener);
        //                LOGGER.fine(name + ": " + errors);
        //                JSONObject err = errors.getJSONObject(0);
        //                throw new Exception(err.getString("msg"));
        //            } else if ("results".equals(name)) {
        //                results = getResults(tokener, type);
        //            } else {
        //                throw tokener.syntaxError(name + ": unanticipated field");
        //            }
        //        }
        //        while (tokener.more() && tokener.skipTo('}') != '}') {
        //            // skip along
        //        }
        //        tokener.next('}');
        //        if (! "success".equals(status)) {
        //            throw new Exception("Unexpected status: '" + status + "'");
        //        }
        //        return IOUtils.toInputStream(results);
        //
    }
    //
    //    private static String getFieldName(JSONTokener tokener) throws JSONException {
    //        char c = tokener.skipTo('"');
    //        if (c != '"') {
    //            return null;
    //        }
    //        tokener.next('"');
    //        return tokener.nextString('"');
    //    }
    //
    //    private static String getStringField(JSONTokener tokener) throws JSONException {
    //        tokener.skipTo('"');
    //        tokener.next('"');
    //        return tokener.nextString('"');
    //    }
    //
    //    private static JSONArray getArrayField(JSONTokener tokener) throws JSONException {
    //        tokener.skipTo(':');
    //        tokener.next(':');
    //        Object obj = tokener.nextValue();
    //        if (obj instanceof JSONArray) {
    //            return (JSONArray) obj;
    //        } else {
    //            throw tokener.syntaxError(String.valueOf(obj) + ": unexpected value");
    //        }
    //    }
    //
    //    private static JSONObject getObjectField(JSONTokener tokener) throws JSONException {
    //        tokener.skipTo(':');
    //        tokener.next(':');
    //        Object obj = tokener.nextValue();
    //        if (obj instanceof JSONObject) {
    //            return (JSONObject) obj;
    //        } else {
    //            throw tokener.syntaxError(String.valueOf(obj) + ": unexpected value");
    //        }
    //    }
    //
    //    private static String getResults(JSONTokener tokener, String type) throws JSONException {
    //        tokener.skipTo(':');
    //        tokener.next(':');
    //        StringBuilder result = new StringBuilder();
    //        if (type != null) {
    //            // a type was provided in the response and so the result is encoded as an array of escaped strings that
    //            // need to be concatenated
    //            Object obj = tokener.nextValue();
    //            if (!(obj instanceof JSONArray)) {
    //                throw tokener.syntaxError("array expected");
    //            }
    //            JSONArray array = (JSONArray) obj;
    //            for (int i = 0; i < array.length(); ++i) {
    //                result.append(array.getString(i));
    //            }
    //            return result.toString();
    //        } else {
    //            int level = 0;
    //            boolean inQuote = false;
    //            while (tokener.more()) {
    //                char c = tokener.next();
    //                switch (c) {
    //                    case '{':
    //                    case '[':
    //                        ++level;
    //                        result.append(c);
    //                        break;
    //                    case '}':
    //                    case ']':
    //                        --level;
    //                        result.append(c);
    //                        break;
    //                    case '"':
    //                        if (inQuote) {
    //                            --level;
    //                        } else {
    //                            ++level;
    //                        }
    //                        inQuote = !inQuote;
    //                        result.append(c);
    //                        break;
    //                    case ',':
    //                        if (level == 0) {
    //                            return result.toString().trim();
    //                        } else {
    //                            result.append(c);
    //                        }
    //                        break;
    //                    default:
    //                        result.append(c);
    //                        break;
    //                }
    //            }
    //        }
    //        return null;
    //    }
}
