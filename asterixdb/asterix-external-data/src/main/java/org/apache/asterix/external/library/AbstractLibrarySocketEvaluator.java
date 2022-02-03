package org.apache.asterix.external.library;

import static org.apache.asterix.common.exceptions.ErrorCode.EXTERNAL_UDF_EXCEPTION;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.IExternalLangIPCProto;
import org.apache.asterix.external.api.ILibraryEvaluator;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;

public abstract class AbstractLibrarySocketEvaluator extends AbstractStateObject implements ILibraryEvaluator {

        protected IExternalLangIPCProto proto;
        protected TaskAttemptId task;
        protected IWarningCollector warningCollector;
        protected SourceLocation sourceLoc;

        public AbstractLibrarySocketEvaluator(JobId jobId, PythonLibraryEvaluatorId evaluatorId, TaskAttemptId task, IWarningCollector warningCollector, SourceLocation sourceLoc) {
                super(jobId,evaluatorId);
                this.task = task;
                this.warningCollector = warningCollector;
                this.sourceLoc = sourceLoc;
        }

        @Override
        public long initialize(IExternalFunctionInfo finfo) throws IOException, AsterixException {
            List<String> externalIdents = finfo.getExternalIdentifier();
            String packageModule = externalIdents.get(0);
            String clazz;
            String fn;
            String externalIdent1 = externalIdents.get(1);
            int idx = externalIdent1.lastIndexOf('.');
            if (idx >= 0) {
                clazz = externalIdent1.substring(0, idx);
                fn = externalIdent1.substring(idx + 1);
            } else {
                clazz = null;
                fn = externalIdent1;
            }
            return proto.init(packageModule, clazz, fn);
        }

        @Override
        public ByteBuffer callPython(long id, IAType[] argTypes, IValueReference[] valueReferences, boolean nullCall)
                throws IOException {
            ByteBuffer ret = null;
            try {
                ret = proto.call(id, argTypes, valueReferences, nullCall);
            } catch (AsterixException e) {
                if (warningCollector.shouldWarn()) {
                    warningCollector.warn(Warning.of(sourceLoc, EXTERNAL_UDF_EXCEPTION, e.getMessage()));
                }
            }
            return ret;
        }

        @Override
        public ByteBuffer callPythonMulti(long id, ArrayBackedValueStorage arguments, int numTuples) throws IOException {
            ByteBuffer ret = null;
            try {
                ret = proto.callMulti(id, arguments, numTuples);
            } catch (AsterixException e) {
                if (warningCollector.shouldWarn()) {
                    warningCollector.warn(Warning.of(sourceLoc, EXTERNAL_UDF_EXCEPTION, e.getMessage()));
                }
            }
            return ret;
        }
}
