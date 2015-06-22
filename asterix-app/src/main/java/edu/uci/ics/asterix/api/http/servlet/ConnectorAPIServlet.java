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

package edu.uci.ics.asterix.api.http.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

/***
 * The REST API that takes a dataverse name and a dataset name as the input
 * and returns an array of file splits (IP, file-path) of the dataset in JSON.
 * It is mostly used by external runtime, e.g., Pregelix or IMRU to pull data
 * in parallel from existing AsterixDB datasets.
 *
 * @author yingyi
 */
public class ConnectorAPIServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private static final String HYRACKS_CONNECTION_ATTR = "edu.uci.ics.asterix.HYRACKS_CONNECTION";

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("text/html");
        response.setCharacterEncoding("utf-8");
        PrintWriter out = response.getWriter();
        try {
            JSONObject jsonResponse = new JSONObject();
            String dataverseName = request.getParameter("dataverseName");
            String datasetName = request.getParameter("datasetName");
            if (dataverseName == null || datasetName == null) {
                jsonResponse.put("error", "Parameter dataverseName or datasetName is null,");
                out.write(jsonResponse.toString());
                out.flush();
                return;
            }
            ServletContext context = getServletContext();

            IHyracksClientConnection hcc = null;
            synchronized (context) {
                hcc = (IHyracksClientConnection) context.getAttribute(HYRACKS_CONNECTION_ATTR);
            }

            // Metadata transaction begins.
            MetadataManager.INSTANCE.init();
            MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();

            // Retrieves file splits of the dataset.
            AqlMetadataProvider metadataProvider = new AqlMetadataProvider(null);
            metadataProvider.setMetadataTxnContext(mdTxnCtx);
            Dataset dataset = metadataProvider.findDataset(dataverseName, datasetName);
            if (dataset == null) {
                jsonResponse.put("error", "Dataset " + datasetName + " does not exist in " + "dataverse "
                        + dataverseName);
                out.write(jsonResponse.toString());
                out.flush();
                return;
            }
            boolean temp = dataset.getDatasetDetails().isTemp();
            FileSplit[] fileSplits = metadataProvider.splitsForDataset(mdTxnCtx, dataverseName, datasetName,
                    datasetName, temp);

            // Constructs the returned json object.
            formResponseObject(jsonResponse, fileSplits, hcc.getNodeControllerInfos());
            // Metadata transaction commits.
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            // Writes file splits.
            out.write(jsonResponse.toString());
            out.flush();
        } catch (Exception e) {
            e.printStackTrace();
            out.println(e.getMessage());
            out.flush();
            e.printStackTrace(out);
        }
    }

    private void formResponseObject(JSONObject jsonResponse, FileSplit[] fileSplits,
            Map<String, NodeControllerInfo> nodeMap) throws Exception {
        JSONArray partititons = new JSONArray();
        // Generates file partitions.
        for (FileSplit split : fileSplits) {
            String ipAddress = nodeMap.get(split.getNodeName()).getNetworkAddress().getAddress().toString();
            String path = split.getLocalFile().getFile().getAbsolutePath();
            FilePartition partition = new FilePartition(ipAddress, path);
            partititons.put(partition.toJSONObject());
        }
        // Generates the response object which contains the splits.
        jsonResponse.put("splits", partititons);
    }
}

class FilePartition {
    private final String ipAddress;
    private final String path;

    public FilePartition(String ipAddress, String path) {
        this.ipAddress = ipAddress;
        this.path = path;
    }

    public String getIPAddress() {
        return ipAddress;
    }

    public String getPath() {
        return path;
    }

    @Override
    public String toString() {
        return ipAddress + ":" + path;
    }

    public JSONObject toJSONObject() throws JSONException {
        JSONObject partition = new JSONObject();
        partition.put("ip", ipAddress);
        partition.put("path", path);
        return partition;
    }
}