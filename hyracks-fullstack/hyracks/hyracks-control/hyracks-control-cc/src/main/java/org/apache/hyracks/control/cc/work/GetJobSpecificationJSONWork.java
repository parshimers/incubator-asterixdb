package org.apache.hyracks.control.cc.work;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.common.work.SynchronizableWork;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class GetJobSpecificationJSONWork extends SynchronizableWork {
    private final ClusterControllerService ccs;
    private final JobId jobId;
    private ObjectNode json;

    public GetJobSpecificationJSONWork(ClusterControllerService ccs, JobId jobId) {
        this.ccs = ccs;
        this.jobId = jobId;
    }

    @Override
    protected void doRun() throws Exception {
        IJobManager jobManager = ccs.getJobManager();
        ObjectMapper om = new ObjectMapper();
        JobRun run = jobManager.get(jobId);
        if (run == null) {
            json = om.createObjectNode();
            return;
        }
        json = run.getJobSpecification().toJSON();
    }

    public ObjectNode getJSON() {
        return json;
    }
}
