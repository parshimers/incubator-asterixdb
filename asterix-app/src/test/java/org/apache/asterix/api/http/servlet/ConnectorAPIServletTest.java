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

package org.apache.asterix.api.http.servlet;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import junit.extensions.PA;
import junit.framework.Assert;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.Test;

import org.apache.asterix.test.runtime.ExecutionTest;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.dataflow.std.file.FileSplit;

public class ConnectorAPIServletTest {

    @Test
    public void testGet() throws Exception {
        // Starts test asterixdb cluster.
        ExecutionTest.setUp();

        // Configures a test connector api servlet.
        ConnectorAPIServlet servlet = spy(new ConnectorAPIServlet());
        ServletConfig mockServletConfig = mock(ServletConfig.class);
        servlet.init(mockServletConfig);
        Map<String, NodeControllerInfo> nodeMap = new HashMap<String, NodeControllerInfo>();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintWriter outputWriter = new PrintWriter(outputStream);

        // Creates mocks.
        ServletContext mockContext = mock(ServletContext.class);
        IHyracksClientConnection mockHcc = mock(IHyracksClientConnection.class);
        NodeControllerInfo mockInfo1 = mock(NodeControllerInfo.class);
        NodeControllerInfo mockInfo2 = mock(NodeControllerInfo.class);
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        HttpServletResponse mockResponse = mock(HttpServletResponse.class);

        // Sets up mock returns.
        when(servlet.getServletContext()).thenReturn(mockContext);
        when(mockRequest.getParameter("dataverseName")).thenReturn("Metadata");
        when(mockRequest.getParameter("datasetName")).thenReturn("Dataset");
        when(mockResponse.getWriter()).thenReturn(outputWriter);
        when(mockContext.getAttribute(anyString())).thenReturn(mockHcc);
        when(mockHcc.getNodeControllerInfos()).thenReturn(nodeMap);
        when(mockInfo1.getNetworkAddress()).thenReturn(new NetworkAddress("127.0.0.1", 3099));
        when(mockInfo2.getNetworkAddress()).thenReturn(new NetworkAddress("127.0.0.2", 3099));

        // Calls ConnectorAPIServlet.formResponseObject.
        nodeMap.put("nc1", mockInfo1);
        nodeMap.put("nc2", mockInfo2);
        servlet.doGet(mockRequest, mockResponse);

        // Constructs the actual response.
        JSONTokener tokener = new JSONTokener(new InputStreamReader(
                new ByteArrayInputStream(outputStream.toByteArray())));
        JSONObject actualResponse = new JSONObject(tokener);

        // Checks the correctness of results.
        JSONArray splits = actualResponse.getJSONArray("splits");
        String path = ((JSONObject) splits.get(0)).getString("path");
        Assert.assertTrue(path.endsWith("Metadata/Dataset_idx_Dataset"));

        // Tears down the asterixdb cluster.
        ExecutionTest.tearDown();
    }

    @Test
    public void testFormResponseObject() throws JSONException {
        ConnectorAPIServlet servlet = new ConnectorAPIServlet();
        JSONObject actualResponse = new JSONObject();
        FileSplit[] splits = new FileSplit[2];
        splits[0] = new FileSplit("nc1", "foo1");
        splits[1] = new FileSplit("nc2", "foo2");
        Map<String, NodeControllerInfo> nodeMap = new HashMap<String, NodeControllerInfo>();
        NodeControllerInfo mockInfo1 = mock(NodeControllerInfo.class);
        NodeControllerInfo mockInfo2 = mock(NodeControllerInfo.class);

        // Sets up mock returns.
        when(mockInfo1.getNetworkAddress()).thenReturn(new NetworkAddress("127.0.0.1", 3099));
        when(mockInfo2.getNetworkAddress()).thenReturn(new NetworkAddress("127.0.0.2", 3099));

        // Calls ConnectorAPIServlet.formResponseObject.
        nodeMap.put("nc1", mockInfo1);
        nodeMap.put("nc2", mockInfo2);
        PA.invokeMethod(servlet,
                "formResponseObject(org.json.JSONObject, org.apache.hyracks.dataflow.std.file.FileSplit[], "
                        + "java.util.Map)", actualResponse, splits, nodeMap);

        // Constructs expected response.
        JSONObject expectedResponse = new JSONObject();
        JSONArray splitsArray = new JSONArray();
        JSONObject element1 = new JSONObject();
        element1.put("ip", "127.0.0.1");
        element1.put("path", splits[0].getLocalFile().getFile().getAbsolutePath());
        JSONObject element2 = new JSONObject();
        element2.put("ip", "127.0.0.2");
        element2.put("path", splits[1].getLocalFile().getFile().getAbsolutePath());
        splitsArray.put(element1);
        splitsArray.put(element2);
        expectedResponse.put("splits", splitsArray);

        // Checks results.
        Assert.assertEquals(actualResponse.toString(), expectedResponse.toString());
    }
}
