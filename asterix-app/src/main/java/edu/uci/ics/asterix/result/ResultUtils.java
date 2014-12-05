/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.result;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.ParseException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.api.common.APIFramework;
import edu.uci.ics.asterix.api.http.servlet.APIServlet;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

public class ResultUtils {
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    static Map<Character, String> HTML_ENTITIES = new HashMap<Character, String>();

    static {
        HTML_ENTITIES.put('"', "&quot;");
        HTML_ENTITIES.put('&', "&amp;");
        HTML_ENTITIES.put('<', "&lt;");
        HTML_ENTITIES.put('>', "&gt;");
    }

    public static String escapeHTML(String s) {
        for (Character c : HTML_ENTITIES.keySet()) {
            if (s.indexOf(c) >= 0) {
                s = s.replace(c.toString(), HTML_ENTITIES.get(c));
            }
        }
        return s;
    }

    public static void displayResults(ResultReader resultReader, PrintWriter out, APIFramework.OutputFormat pdf)
            throws HyracksDataException {
        IFrameTupleAccessor fta = resultReader.getFrameTupleAccessor();

        ByteBuffer buffer = ByteBuffer.allocate(ResultReader.FRAME_SIZE);
        buffer.clear();

        int bytesRead = resultReader.read(buffer);
        ByteBufferInputStream bbis = new ByteBufferInputStream();
        boolean need_commas = true;
        boolean notfirst = false;
        switch (pdf) {
            case HTML:
                out.println("<h4>Results:</h4>");
                out.println("<pre>");
                need_commas = false;
                break;
            case JSON:
            case ADM:
                // Conveniently, JSON and ADM have the same syntax for an
                // "ordered list", and our representation of the result of a
                // statement is an ordered list of instances.
                out.print("[ ");
                break;
        }
        if (bytesRead > 0) {
            do {
                try {
                    fta.reset(buffer);
                    int last = fta.getTupleCount();
                    String result;
                    for (int tIndex = 0; tIndex < last; tIndex++) {
                        int start = fta.getTupleStartOffset(tIndex);
                        int length = fta.getTupleEndOffset(tIndex) - start;
                        bbis.setByteBuffer(buffer, start);
                        byte[] recordBytes = new byte[length];
                        bbis.read(recordBytes, 0, length);
                        result = new String(recordBytes, 0, length, UTF_8);
                        if (need_commas && notfirst) {
                            out.print(", ");
                        }
                        notfirst = true;
                        out.print(result);
                    }
                    buffer.clear();
                } finally {
                    try {
                        bbis.close();
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                }
            } while (resultReader.read(buffer) > 0);
        }

        out.flush();

        switch (pdf) {
            case HTML:
                out.println("</pre>");
                break;
            case JSON:
            case ADM:
                out.println(" ]");
                break;
        }

    }

    public static JSONObject getErrorResponse(int errorCode, String errorMessage, String errorSummary,
            String errorStackTrace) {
        JSONObject errorResp = new JSONObject();
        JSONArray errorArray = new JSONArray();
        errorArray.put(errorCode);
        errorArray.put(errorMessage);
        try {
            errorResp.put("error-code", errorArray);
            if (!errorSummary.equals(""))
                errorResp.put("summary", errorSummary);
            if (!errorStackTrace.equals(""))
                errorResp.put("stacktrace", errorStackTrace);
        } catch (JSONException e) {
            // TODO(madhusudancs): Figure out what to do when JSONException occurs while building the results.
        }
        return errorResp;
    }

    public static void webUIErrorHandler(PrintWriter out, Exception e) {
        String errorTemplate = readTemplateFile("/webui/errortemplate.html", "%s\n%s\n%s");

        String errorOutput = String.format(errorTemplate, escapeHTML(extractErrorMessage(e)),
                escapeHTML(extractErrorSummary(e)), escapeHTML(extractFullStackTrace(e)));
        out.println(errorOutput);
    }

    public static void webUIParseExceptionHandler(PrintWriter out, Throwable e, String query) {
        String errorTemplate = readTemplateFile("/webui/errortemplate_message.html", "<pre class=\"error\">%s\n</pre>");

        String errorOutput = String.format(errorTemplate, buildParseExceptionMessage(e, query));
        out.println(errorOutput);
    }

    public static void apiErrorHandler(PrintWriter out, Exception e) {
        int errorCode = 99;
        if (e instanceof ParseException) {
            errorCode = 2;
        } else if (e instanceof AlgebricksException) {
            errorCode = 3;
        } else if (e instanceof HyracksDataException) {
            errorCode = 4;
        }

        JSONObject errorResp = ResultUtils.getErrorResponse(errorCode, extractErrorMessage(e), extractErrorSummary(e),
                extractFullStackTrace(e));
        out.write(errorResp.toString());
    }

    public static String buildParseExceptionMessage(Throwable e, String query) {
        StringBuilder errorMessage = new StringBuilder();
        String message = e.getMessage();
        message = message.replace("<", "&lt");
        message = message.replace(">", "&gt");
        errorMessage.append("SyntaxError: " + message + "\n");
        int pos = message.indexOf("line");
        if (pos > 0) {
            Pattern p = Pattern.compile("\\d+");
            Matcher m = p.matcher(message);
            if (m.find(pos)) {
                int lineNo = Integer.parseInt(message.substring(m.start(), m.end()));
                String[] lines = query.split("\n");
                if (lineNo > lines.length) {
                    errorMessage.append("===> &ltBLANK LINE&gt \n");
                } else {
                    String line = lines[lineNo - 1];
                    errorMessage.append("==> " + line);
                }
            }
        }
        return errorMessage.toString();
    }

    private static Throwable getRootCause(Throwable cause) {
        Throwable nextCause = cause.getCause();
        while (nextCause != null) {
            cause = nextCause;
            nextCause = cause.getCause();
        }
        return cause;
    }

    /**
     * Extract the message in the root cause of the stack trace:
     *
     * @param e
     * @return error message string.
     */
    private static String extractErrorMessage(Throwable e) {
        Throwable cause = getRootCause(e);

        String[] messageSplits = cause.toString().split(":");
        String exceptionClassName = messageSplits[0];
        if (messageSplits.length > 1) {
            String fullyQualifiedExceptionClassName = messageSplits[0];
            System.out.println(fullyQualifiedExceptionClassName);
            String[] hierarchySplits = fullyQualifiedExceptionClassName.split("\\.");
            if (hierarchySplits.length > 0) {
                exceptionClassName = hierarchySplits[hierarchySplits.length - 1];
            }
        }
        String localizedMessage = cause.getLocalizedMessage();
        if(localizedMessage == null){
            localizedMessage = "Guru meditation error. Please check CC and NC logs for stacktraces";
        }
        return localizedMessage + " [" + exceptionClassName + "]";
    }

    /**
     * Extract the meaningful part of a stack trace:
     * a. the causes in the stack trace hierarchy
     * b. the top exception for each cause
     *
     * @param e
     * @return the contacted message containing a and b.
     */
    private static String extractErrorSummary(Throwable e) {
        StringBuilder errorMessageBuilder = new StringBuilder();
        Throwable cause = e;
        errorMessageBuilder.append(cause.getLocalizedMessage());
        while (cause != null) {
            StackTraceElement[] stackTraceElements = cause.getStackTrace();
            errorMessageBuilder.append(stackTraceElements.length > 0 ? "\n caused by: " + stackTraceElements[0] : "");
            cause = cause.getCause();
        }
        return errorMessageBuilder.toString();
    }

    /**
     * Extract the full stack trace:
     *
     * @param e
     * @return the string containing the full stack trace of the error.
     */
    private static String extractFullStackTrace(Throwable e) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        e.printStackTrace(printWriter);
        return stringWriter.toString();
    }

    /**
     * Read the template file which is stored as a resource and return its content. If the file does not exist or is
     * not readable return the default template string.
     *
     * @param path
     *            The path to the resource template file
     * @param defaultTemplate
     *            The default template string if the template file does not exist or is not readable
     * @return The template string to be used to render the output.
     */
    private static String readTemplateFile(String path, String defaultTemplate) {
        String errorTemplate = defaultTemplate;
        try {
            String resourcePath = "/webui/errortemplate_message.html";
            InputStream is = APIServlet.class.getResourceAsStream(resourcePath);
            InputStreamReader isr = new InputStreamReader(is);
            StringBuilder sb = new StringBuilder();
            BufferedReader br = new BufferedReader(isr);
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                line = br.readLine();
            }
            errorTemplate = sb.toString();
        } catch (IOException ioe) {
            // If there is an IOException reading the error template html file, default value of error template is used.
        }
        return errorTemplate;
    }
}
