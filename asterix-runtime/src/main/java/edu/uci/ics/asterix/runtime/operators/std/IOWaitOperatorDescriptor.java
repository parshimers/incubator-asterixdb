package edu.uci.ics.asterix.runtime.operators.std;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.context.DatasetLifecycleManager;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

public class IOWaitOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    public IOWaitOperatorDescriptor(IOperatorDescriptorRegistry spec) {
        super(spec, 0, 0);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new IOWaitOperatorNodePushable(ctx);
    }

    private static class IOWaitOperatorNodePushable extends AbstractOperatorNodePushable {

        private final DatasetLifecycleManager dlm;

        public IOWaitOperatorNodePushable(IHyracksTaskContext ctx) {
            IAsterixAppRuntimeContext rtCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                    .getApplicationContext().getApplicationObject();
            dlm = (DatasetLifecycleManager) rtCtx.getIndexLifecycleManager();
        }

        @Override
        public void initialize() throws HyracksDataException {
            dlm.waitForIO();
        }

        @Override
        public void deinitialize() throws HyracksDataException {
        }

        @Override
        public int getInputArity() {
            return 0;
        }

        @Override
        public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc)
                throws HyracksDataException {

        }

        @Override
        public IFrameWriter getInputFrameWriter(int index) {
            return null;
        }

    }
}
