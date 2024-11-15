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

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

import edu.uci.ics.asterix.result.ResultReader;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.dataset.IHyracksDataset;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.client.dataset.HyracksDataset;

public class QueryStatusAPIServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private static final String HYRACKS_CONNECTION_ATTR = "edu.uci.ics.asterix.HYRACKS_CONNECTION";

    private static final String HYRACKS_DATASET_ATTR = "edu.uci.ics.asterix.HYRACKS_DATASET";

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("text/html");
        response.setCharacterEncoding("utf-8");
        String strHandle = request.getParameter("handle");
        PrintWriter out = response.getWriter();
        ServletContext context = getServletContext();
        IHyracksClientConnection hcc;
        IHyracksDataset hds;

        try {
            synchronized (context) {
                hcc = (IHyracksClientConnection) context.getAttribute(HYRACKS_CONNECTION_ATTR);

                hds = (IHyracksDataset) context.getAttribute(HYRACKS_DATASET_ATTR);
                if (hds == null) {
                    hds = new HyracksDataset(hcc, ResultReader.FRAME_SIZE, ResultReader.NUM_READERS);
                    context.setAttribute(HYRACKS_DATASET_ATTR, hds);
                }
            }
            JSONObject handleObj = new JSONObject(strHandle);
            JSONArray handle = handleObj.getJSONArray("handle");
            JobId jobId = new JobId(handle.getLong(0));
            ResultSetId rsId = new ResultSetId(handle.getLong(1));

            /* TODO(madhusudancs): We need to find a way to JSON serialize default format obtained from
             * metadataProvider in the AQLTranslator and store it as part of the result handle.
             */
            ResultReader resultReader = new ResultReader(hcc, hds);
            resultReader.open(jobId, rsId);

            JSONObject jsonResponse = new JSONObject();
            String status;
            switch (resultReader.getStatus()) {
                case RUNNING:
                    status = "RUNNING";
                    break;
                case FAILED:
                    status = "ERROR";
                    break;
                case SUCCESS:
                    status = "SUCCESS";
                    break;
                default:
                    status = "ERROR";
                    break;
            }
            jsonResponse.put("status", status);
            out.write(jsonResponse.toString());

        } catch (Exception e) {
            out.println(e.getMessage());
            e.printStackTrace(out);
        }
    }
}
