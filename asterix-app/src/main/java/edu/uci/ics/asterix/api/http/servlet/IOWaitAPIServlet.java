package edu.uci.ics.asterix.api.http.servlet;

import java.io.IOException;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.uci.ics.asterix.file.JobSpecificationUtils;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.asterix.runtime.operators.std.IOWaitOperatorDescriptor;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class IOWaitAPIServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    private static final String HYRACKS_CONNECTION_ATTR = "edu.uci.ics.asterix.HYRACKS_CONNECTION";

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        ServletContext context = getServletContext();
        IHyracksClientConnection hcc = null;

        synchronized (context) {
            hcc = (IHyracksClientConnection) context.getAttribute(HYRACKS_CONNECTION_ATTR);
        }

        Map<String, String[]> stores = AsterixAppContextInfo.getInstance().getMetadataProperties().getStores();
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        IOWaitOperatorDescriptor iowod = new IOWaitOperatorDescriptor(spec);
        AlgebricksAbsolutePartitionConstraint aapc = new AlgebricksAbsolutePartitionConstraint(stores.keySet().toArray(
                new String[] {}));
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, iowod, aapc);
        spec.addRoot(iowod);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());

        spec.setMaxReattempts(0);
        try {
            JobId jobId = hcc.startJob(spec);
            hcc.waitForCompletion(jobId);
        } catch (Exception e) {
            e.printStackTrace();
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }
}
