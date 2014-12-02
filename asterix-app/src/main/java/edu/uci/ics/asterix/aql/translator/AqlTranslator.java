/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * Copyright 2009-2013 by The Regents of the University of California
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.aql.translator;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.api.common.APIFramework;
import edu.uci.ics.asterix.api.common.APIFramework.DisplayFormat;
import edu.uci.ics.asterix.api.common.Job;
import edu.uci.ics.asterix.api.common.SessionConfig;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.CompactStatement;
import edu.uci.ics.asterix.aql.expression.ConnectFeedStatement;
import edu.uci.ics.asterix.aql.expression.CreateDataverseStatement;
import edu.uci.ics.asterix.aql.expression.CreateFeedPolicyStatement;
import edu.uci.ics.asterix.aql.expression.CreateFeedStatement;
import edu.uci.ics.asterix.aql.expression.CreateFunctionStatement;
import edu.uci.ics.asterix.aql.expression.CreateIndexStatement;
import edu.uci.ics.asterix.aql.expression.CreatePrimaryFeedStatement;
import edu.uci.ics.asterix.aql.expression.CreateSecondaryFeedStatement;
import edu.uci.ics.asterix.aql.expression.DatasetDecl;
import edu.uci.ics.asterix.aql.expression.DataverseDecl;
import edu.uci.ics.asterix.aql.expression.DataverseDropStatement;
import edu.uci.ics.asterix.aql.expression.DeleteStatement;
import edu.uci.ics.asterix.aql.expression.DisconnectFeedStatement;
import edu.uci.ics.asterix.aql.expression.DropStatement;
import edu.uci.ics.asterix.aql.expression.ExternalDetailsDecl;
import edu.uci.ics.asterix.aql.expression.FeedDropStatement;
import edu.uci.ics.asterix.aql.expression.FeedPolicyDropStatement;
import edu.uci.ics.asterix.aql.expression.FunctionDecl;
import edu.uci.ics.asterix.aql.expression.FunctionDropStatement;
import edu.uci.ics.asterix.aql.expression.Identifier;
import edu.uci.ics.asterix.aql.expression.IndexDropStatement;
import edu.uci.ics.asterix.aql.expression.InsertStatement;
import edu.uci.ics.asterix.aql.expression.InternalDetailsDecl;
import edu.uci.ics.asterix.aql.expression.LoadStatement;
import edu.uci.ics.asterix.aql.expression.NodeGroupDropStatement;
import edu.uci.ics.asterix.aql.expression.NodegroupDecl;
import edu.uci.ics.asterix.aql.expression.Query;
import edu.uci.ics.asterix.aql.expression.SetStatement;
import edu.uci.ics.asterix.aql.expression.SubscribeFeedStatement;
import edu.uci.ics.asterix.aql.expression.TypeDecl;
import edu.uci.ics.asterix.aql.expression.TypeDropStatement;
import edu.uci.ics.asterix.aql.expression.WriteStatement;
import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.config.OptimizationConfUtil;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.FeedJointKey;
import edu.uci.ics.asterix.common.feeds.FeedPolicyAccessor;
import edu.uci.ics.asterix.common.feeds.FeedConnectionRequest;
import edu.uci.ics.asterix.common.feeds.api.IFeedJoint;
import edu.uci.ics.asterix.common.feeds.api.IFeedJoint.FeedJointType;
import edu.uci.ics.asterix.common.feeds.api.IFeedLifecycleEventSubscriber;
import edu.uci.ics.asterix.common.feeds.api.IFeedLifecycleListener.ConnectionLocation;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.feeds.FeedJoint;
import edu.uci.ics.asterix.feeds.FeedLifecycleListener;
import edu.uci.ics.asterix.file.DatasetOperations;
import edu.uci.ics.asterix.file.DataverseOperations;
import edu.uci.ics.asterix.file.FeedOperations;
import edu.uci.ics.asterix.file.IndexOperations;
import edu.uci.ics.asterix.formats.base.IDataFormat;
import edu.uci.ics.asterix.metadata.IDatasetDetails;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.api.IMetadataEntity;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataConstants;
import edu.uci.ics.asterix.metadata.dataset.hints.DatasetHints;
import edu.uci.ics.asterix.metadata.dataset.hints.DatasetHints.DatasetNodegroupCardinalityHint;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.CompactionPolicy;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.metadata.entities.ExternalDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.Feed;
import edu.uci.ics.asterix.metadata.entities.Feed.FeedType;
import edu.uci.ics.asterix.metadata.entities.FeedActivity.FeedActivityDetails;
import edu.uci.ics.asterix.metadata.entities.FeedPolicy;
import edu.uci.ics.asterix.metadata.entities.Function;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.metadata.entities.InternalDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.NodeGroup;
import edu.uci.ics.asterix.metadata.entities.PrimaryFeed;
import edu.uci.ics.asterix.metadata.entities.SecondaryFeed;
import edu.uci.ics.asterix.metadata.feeds.FeedLifecycleEventSubscriber;
import edu.uci.ics.asterix.metadata.feeds.FeedUtil;
import edu.uci.ics.asterix.metadata.feeds.IAdapterFactory;
import edu.uci.ics.asterix.metadata.feeds.IFeedAdapterFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.types.TypeSignature;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.asterix.result.ResultReader;
import edu.uci.ics.asterix.result.ResultUtils;
import edu.uci.ics.asterix.transaction.management.service.transaction.DatasetIdFactory;
import edu.uci.ics.asterix.translator.AbstractAqlTranslator;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledConnectFeedStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledCreateIndexStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledDatasetDropStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledDeleteStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledIndexCompactStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledIndexDropStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledInsertStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledLoadFromFileStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledSubscribeFeedStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.ICompiledDmlStatement;
import edu.uci.ics.asterix.translator.TypeTranslator;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;
import edu.uci.ics.hyracks.algebricks.data.IAWriterFactory;
import edu.uci.ics.hyracks.algebricks.data.IResultSerializerFactoryProvider;
import edu.uci.ics.hyracks.algebricks.runtime.serializer.ResultSerializerFactoryProvider;
import edu.uci.ics.hyracks.algebricks.runtime.writers.PrinterBasedWriterFactory;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.dataset.IHyracksDataset;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;

/*
 * Provides functionality for executing a batch of AQL statements (queries included)
 * sequentially.
 */
public class AqlTranslator extends AbstractAqlTranslator {

    private static Logger LOGGER = Logger.getLogger(AqlTranslator.class.getName());

    private enum ProgressState {
        NO_PROGRESS,
        ADDED_PENDINGOP_RECORD_TO_METADATA
    }

    public static final boolean IS_DEBUG_MODE = false;//true
    private final List<Statement> aqlStatements;
    private final PrintWriter out;
    private final SessionConfig sessionConfig;
    private final DisplayFormat pdf;
    private Dataverse activeDefaultDataverse;
    private List<FunctionDecl> declaredFunctions;

    public AqlTranslator(List<Statement> aqlStatements, PrintWriter out, SessionConfig pc, DisplayFormat pdf)
            throws MetadataException, AsterixException {
        this.aqlStatements = aqlStatements;
        this.out = out;
        this.sessionConfig = pc;
        this.pdf = pdf;
        declaredFunctions = getDeclaredFunctions(aqlStatements);
    }

    private List<FunctionDecl> getDeclaredFunctions(List<Statement> statements) {
        List<FunctionDecl> functionDecls = new ArrayList<FunctionDecl>();
        for (Statement st : statements) {
            if (st.getKind().equals(Statement.Kind.FUNCTION_DECL)) {
                functionDecls.add((FunctionDecl) st);
            }
        }
        return functionDecls;
    }

    /**
     * Compiles and submits for execution a list of AQL statements.
     * 
     * @param hcc
     *            A Hyracks client connection that is used to submit a jobspec to Hyracks.
     * @param hdc
     *            A Hyracks dataset client object that is used to read the results.
     * @param asyncResults
     *            True if the results should be read asynchronously or false if we should wait for results to be read.
     * @return A List<QueryResult> containing a QueryResult instance corresponding to each submitted query.
     * @throws Exception
     */
    public List<QueryResult> compileAndExecute(IHyracksClientConnection hcc, IHyracksDataset hdc, boolean asyncResults)
            throws Exception {
        int resultSetIdCounter = 0;
        List<QueryResult> executionResult = new ArrayList<QueryResult>();
        FileSplit outputFile = null;
        IAWriterFactory writerFactory = PrinterBasedWriterFactory.INSTANCE;
        IResultSerializerFactoryProvider resultSerializerFactoryProvider = ResultSerializerFactoryProvider.INSTANCE;
        Map<String, String> config = new HashMap<String, String>();
        for (Statement stmt : aqlStatements) {
            validateOperation(activeDefaultDataverse, stmt);
            AqlMetadataProvider metadataProvider = new AqlMetadataProvider(activeDefaultDataverse);
            metadataProvider.setWriterFactory(writerFactory);
            metadataProvider.setResultSerializerFactoryProvider(resultSerializerFactoryProvider);
            metadataProvider.setOutputFile(outputFile);
            metadataProvider.setConfig(config);
            switch (stmt.getKind()) {
                case SET: {
                    handleSetStatement(metadataProvider, stmt, config);
                    break;
                }
                case DATAVERSE_DECL: {
                    activeDefaultDataverse = handleUseDataverseStatement(metadataProvider, stmt);
                    break;
                }
                case CREATE_DATAVERSE: {
                    handleCreateDataverseStatement(metadataProvider, stmt);
                    break;
                }
                case DATASET_DECL: {
                    handleCreateDatasetStatement(metadataProvider, stmt, hcc);
                    break;
                }
                case CREATE_INDEX: {
                    handleCreateIndexStatement(metadataProvider, stmt, hcc);
                    break;
                }
                case TYPE_DECL: {
                    handleCreateTypeStatement(metadataProvider, stmt);
                    break;
                }
                case NODEGROUP_DECL: {
                    handleCreateNodeGroupStatement(metadataProvider, stmt);
                    break;
                }
                case DATAVERSE_DROP: {
                    handleDataverseDropStatement(metadataProvider, stmt, hcc);
                    break;
                }
                case DATASET_DROP: {
                    handleDatasetDropStatement(metadataProvider, stmt, hcc);
                    break;
                }
                case INDEX_DROP: {
                    handleIndexDropStatement(metadataProvider, stmt, hcc);
                    break;
                }
                case TYPE_DROP: {
                    handleTypeDropStatement(metadataProvider, stmt);
                    break;
                }
                case NODEGROUP_DROP: {
                    handleNodegroupDropStatement(metadataProvider, stmt);
                    break;
                }

                case CREATE_FUNCTION: {
                    handleCreateFunctionStatement(metadataProvider, stmt);
                    break;
                }

                case FUNCTION_DROP: {
                    handleFunctionDropStatement(metadataProvider, stmt);
                    break;
                }

                case LOAD: {
                    handleLoadStatement(metadataProvider, stmt, hcc);
                    break;
                }
                case INSERT: {
                    handleInsertStatement(metadataProvider, stmt, hcc);
                    break;
                }
                case DELETE: {
                    handleDeleteStatement(metadataProvider, stmt, hcc);
                    break;
                }

                case CREATE_PRIMARY_FEED:
                case CREATE_SECONDARY_FEED:
                    handleCreateFeedStatement(metadataProvider, stmt, hcc);
                    break;

                case DROP_FEED: {
                    handleDropFeedStatement(metadataProvider, stmt, hcc);
                    break;
                }

                case DROP_FEED_POLICY: {
                    handleDropFeedPolicyStatement(metadataProvider, stmt, hcc);
                    break;
                }

                case CONNECT_FEED: {
                    handleConnectFeedStatement(metadataProvider, stmt, hcc);
                    break;
                }

                case DISCONNECT_FEED: {
                    handleDisconnectFeedStatement(metadataProvider, stmt, hcc);
                    break;
                }

                case SUBSCRIBE_FEED: {
                    handleSubscribeFeedStatement(metadataProvider, stmt, hcc);
                    break;
                }

                case CREATE_FEED_POLICY: {
                    handleCreateFeedPolicyStatement(metadataProvider, stmt, hcc);
                    break;
                }

                case QUERY: {
                    metadataProvider.setResultSetId(new ResultSetId(resultSetIdCounter++));
                    metadataProvider.setResultAsyncMode(asyncResults);
                    executionResult.add(handleQuery(metadataProvider, (Query) stmt, hcc, hdc, asyncResults));
                    break;
                }

                case COMPACT: {
                    handleCompactStatement(metadataProvider, stmt, hcc);
                    break;
                }

                case WRITE: {
                    Pair<IAWriterFactory, FileSplit> result = handleWriteStatement(metadataProvider, stmt);
                    if (result.first != null) {
                        writerFactory = result.first;
                    }
                    outputFile = result.second;
                    break;
                }
            }
        }
        return executionResult;
    }

    private void handleSetStatement(AqlMetadataProvider metadataProvider, Statement stmt, Map<String, String> config)
            throws RemoteException, ACIDException {
        SetStatement ss = (SetStatement) stmt;
        String pname = ss.getPropName();
        String pvalue = ss.getPropValue();
        config.put(pname, pvalue);
    }

    private Pair<IAWriterFactory, FileSplit> handleWriteStatement(AqlMetadataProvider metadataProvider, Statement stmt)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        WriteStatement ws = (WriteStatement) stmt;
        File f = new File(ws.getFileName());
        FileSplit outputFile = new FileSplit(ws.getNcName().getValue(), new FileReference(f));
        IAWriterFactory writerFactory = null;
        if (ws.getWriterClassName() != null) {
            writerFactory = (IAWriterFactory) Class.forName(ws.getWriterClassName()).newInstance();
        }
        return new Pair<IAWriterFactory, FileSplit>(writerFactory, outputFile);
    }

    private Dataverse handleUseDataverseStatement(AqlMetadataProvider metadataProvider, Statement stmt)
            throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireReadLatch();

        try {
            DataverseDecl dvd = (DataverseDecl) stmt;
            String dvName = dvd.getDataverseName().getValue();
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(), dvName);
            if (dv == null) {
                throw new MetadataException("Unknown dataverse " + dvName);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return dv;
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw new MetadataException(e);
        } finally {
            releaseReadLatch();
        }
    }

    private void handleCreateDataverseStatement(AqlMetadataProvider metadataProvider, Statement stmt) throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        try {
            CreateDataverseStatement stmtCreateDataverse = (CreateDataverseStatement) stmt;
            String dvName = stmtCreateDataverse.getDataverseName().getValue();
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(), dvName);
            if (dv != null) {
                if (stmtCreateDataverse.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("A dataverse with this name " + dvName + " already exists.");
                }
            }
            MetadataManager.INSTANCE.addDataverse(metadataProvider.getMetadataTxnContext(), new Dataverse(dvName,
                    stmtCreateDataverse.getFormat(), IMetadataEntity.PENDING_NO_OP));
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private void validateCompactionPolicy(String compactionPolicy, Map<String, String> compactionPolicyProperties,
            MetadataTransactionContext mdTxnCtx) throws AsterixException, Exception {
        CompactionPolicy compactionPolicyEntity = MetadataManager.INSTANCE.getCompactionPolicy(mdTxnCtx,
                MetadataConstants.METADATA_DATAVERSE_NAME, compactionPolicy);
        if (compactionPolicyEntity == null) {
            throw new AsterixException("Unknown compaction policy :" + compactionPolicy);
        }
        String compactionPolicyFactoryClassName = compactionPolicyEntity.getClassName();
        ILSMMergePolicyFactory mergePolicyFactory = (ILSMMergePolicyFactory) Class.forName(
                compactionPolicyFactoryClassName).newInstance();
        for (Map.Entry<String, String> entry : compactionPolicyProperties.entrySet()) {
            if (!mergePolicyFactory.getPropertiesNames().contains(entry.getKey())) {
                throw new AsterixException("Invalid compaction policy property :" + entry.getKey());
            }
        }
    }

    private void handleCreateDatasetStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws AsterixException, Exception {

        ProgressState progress = ProgressState.NO_PROGRESS;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        String dataverseName = null;
        String datasetName = null;
        Dataset dataset = null;
        try {
            DatasetDecl dd = (DatasetDecl) stmt;
            dataverseName = getActiveDataverse(dd.getDataverse());
            datasetName = dd.getName().getValue();

            DatasetType dsType = dd.getDatasetType();
            String itemTypeName = dd.getItemTypeName().getValue();

            IDatasetDetails datasetDetails = null;
            Dataset ds = MetadataManager.INSTANCE.getDataset(metadataProvider.getMetadataTxnContext(), dataverseName,
                    datasetName);
            if (ds != null) {
                if (dd.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("A dataset with this name " + datasetName + " already exists.");
                }
            }
            Datatype dt = MetadataManager.INSTANCE.getDatatype(metadataProvider.getMetadataTxnContext(), dataverseName,
                    itemTypeName);
            if (dt == null) {
                throw new AlgebricksException(": type " + itemTypeName + " could not be found.");
            }
            switch (dd.getDatasetType()) {
                case INTERNAL: {
                    IAType itemType = dt.getDatatype();
                    if (itemType.getTypeTag() != ATypeTag.RECORD) {
                        throw new AlgebricksException("Can only partition ARecord's.");
                    }
                    List<String> partitioningExprs = ((InternalDetailsDecl) dd.getDatasetDetailsDecl())
                            .getPartitioningExprs();
                    boolean autogenerated = ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).isAutogenerated();
                    ARecordType aRecordType = (ARecordType) itemType;
                    aRecordType.validatePartitioningExpressions(partitioningExprs, autogenerated);

                    Identifier ngNameId = ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).getNodegroupName();
                    String ngName = ngNameId != null ? ngNameId.getValue() : configureNodegroupForDataset(dd,
                            dataverseName, mdTxnCtx);

                    String compactionPolicy = ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).getCompactionPolicy();
                    Map<String, String> compactionPolicyProperties = ((InternalDetailsDecl) dd.getDatasetDetailsDecl())
                            .getCompactionPolicyProperties();
                    if (compactionPolicy == null) {
                        compactionPolicy = GlobalConfig.DEFAULT_COMPACTION_POLICY_NAME;
                        compactionPolicyProperties = GlobalConfig.DEFAULT_COMPACTION_POLICY_PROPERTIES;
                    } else {
                        validateCompactionPolicy(compactionPolicy, compactionPolicyProperties, mdTxnCtx);
                    }
                    datasetDetails = new InternalDatasetDetails(InternalDatasetDetails.FileStructure.BTREE,
                            InternalDatasetDetails.PartitioningStrategy.HASH, partitioningExprs, partitioningExprs,
                            ngName, autogenerated, compactionPolicy, compactionPolicyProperties);
                    break;
                }
                case EXTERNAL: {
                    String adapter = ((ExternalDetailsDecl) dd.getDatasetDetailsDecl()).getAdapter();
                    Map<String, String> properties = ((ExternalDetailsDecl) dd.getDatasetDetailsDecl()).getProperties();
                    datasetDetails = new ExternalDatasetDetails(adapter, properties);
                    break;
                }

            }

            //#. initialize DatasetIdFactory if it is not initialized.
            if (!DatasetIdFactory.isInitialized()) {
                DatasetIdFactory.initialize(MetadataManager.INSTANCE.getMostRecentDatasetId());
            }

            //#. add a new dataset with PendingAddOp
            dataset = new Dataset(dataverseName, datasetName, itemTypeName, datasetDetails, dd.getHints(), dsType,
                    DatasetIdFactory.generateDatasetId(), IMetadataEntity.PENDING_ADD_OP);
            MetadataManager.INSTANCE.addDataset(metadataProvider.getMetadataTxnContext(), dataset);

            if (dd.getDatasetType() == DatasetType.INTERNAL) {
                Dataverse dataverse = MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(),
                        dataverseName);
                JobSpecification jobSpec = DatasetOperations.createDatasetJobSpec(dataverse, datasetName,
                        metadataProvider);

                //#. make metadataTxn commit before calling runJob.
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                bActiveTxn = false;
                progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

                //#. runJob
                runJob(hcc, jobSpec, true);

                //#. begin new metadataTxn
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
            }

            //#. add a new dataset with PendingNoOp after deleting the dataset with PendingAddOp
            MetadataManager.INSTANCE.dropDataset(metadataProvider.getMetadataTxnContext(), dataverseName, datasetName);
            dataset.setPendingOp(IMetadataEntity.PENDING_NO_OP);
            MetadataManager.INSTANCE.addDataset(metadataProvider.getMetadataTxnContext(), dataset);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }

            if (progress == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {

                //#. execute compensation operations
                //   remove the index in NC
                //   [Notice]
                //   As long as we updated(and committed) metadata, we should remove any effect of the job 
                //   because an exception occurs during runJob.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                CompiledDatasetDropStatement cds = new CompiledDatasetDropStatement(dataverseName, datasetName);
                try {
                    JobSpecification jobSpec = DatasetOperations.createDropDatasetJobSpec(cds, metadataProvider);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    bActiveTxn = false;

                    runJob(hcc, jobSpec, true);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    if (bActiveTxn) {
                        abort(e, e2, mdTxnCtx);
                    }
                }

                //   remove the record from the metadata.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                try {
                    MetadataManager.INSTANCE.dropDataset(metadataProvider.getMetadataTxnContext(), dataverseName,
                            datasetName);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    throw new IllegalStateException("System is inconsistent state: pending dataset(" + dataverseName
                            + "." + datasetName + ") couldn't be removed from the metadata", e);
                }
            }

            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private String configureNodegroupForDataset(DatasetDecl dd, String dataverse, MetadataTransactionContext mdTxnCtx)
            throws AsterixException {
        int nodegroupCardinality = -1;
        String nodegroupName;
        String hintValue = dd.getHints().get(DatasetNodegroupCardinalityHint.NAME);
        if (hintValue == null) {
            nodegroupName = MetadataConstants.METADATA_DEFAULT_NODEGROUP_NAME;
            return nodegroupName;
        } else {
            int numChosen = 0;
            boolean valid = DatasetHints.validate(DatasetNodegroupCardinalityHint.NAME,
                    dd.getHints().get(DatasetNodegroupCardinalityHint.NAME)).first;
            if (!valid) {
                throw new AsterixException("Incorrect use of hint:" + DatasetNodegroupCardinalityHint.NAME);
            } else {
                nodegroupCardinality = Integer.parseInt(dd.getHints().get(DatasetNodegroupCardinalityHint.NAME));
            }
            Set<String> nodeNames = AsterixAppContextInfo.getInstance().getMetadataProperties().getNodeNames();
            Set<String> nodeNamesClone = new HashSet<String>();
            for (String node : nodeNames) {
                nodeNamesClone.add(node);
            }
            String metadataNodeName = AsterixAppContextInfo.getInstance().getMetadataProperties().getMetadataNodeName();
            List<String> selectedNodes = new ArrayList<String>();
            selectedNodes.add(metadataNodeName);
            numChosen++;
            nodeNamesClone.remove(metadataNodeName);

            if (numChosen < nodegroupCardinality) {
                Random random = new Random();
                String[] nodes = nodeNamesClone.toArray(new String[] {});
                int[] b = new int[nodeNamesClone.size()];
                for (int i = 0; i < b.length; i++) {
                    b[i] = i;
                }

                for (int i = 0; i < nodegroupCardinality - numChosen; i++) {
                    int selected = i + random.nextInt(nodeNamesClone.size() - i);
                    int selNodeIndex = b[selected];
                    selectedNodes.add(nodes[selNodeIndex]);
                    int temp = b[0];
                    b[0] = b[selected];
                    b[selected] = temp;
                }
            }
            nodegroupName = dataverse + ":" + dd.getName().getValue();
            MetadataManager.INSTANCE.addNodegroup(mdTxnCtx, new NodeGroup(nodegroupName, selectedNodes));
            return nodegroupName;
        }

    }

    private void handleCreateIndexStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {

        ProgressState progress = ProgressState.NO_PROGRESS;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        String dataverseName = null;
        String datasetName = null;
        String indexName = null;
        JobSpecification spec = null;
        Dataset ds = null;
        try {
            CreateIndexStatement stmtCreateIndex = (CreateIndexStatement) stmt;
            dataverseName = getActiveDataverse(stmtCreateIndex.getDataverseName());
            datasetName = stmtCreateIndex.getDatasetName().getValue();

            ds = MetadataManager.INSTANCE.getDataset(metadataProvider.getMetadataTxnContext(), dataverseName,
                    datasetName);
            if (ds == null) {
                throw new AlgebricksException("There is no dataset with this name " + datasetName + " in dataverse "
                        + dataverseName);
            }

            indexName = stmtCreateIndex.getIndexName().getValue();
            Index idx = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(), dataverseName,
                    datasetName, indexName);

            String itemTypeName = ds.getItemTypeName();
            Datatype dt = MetadataManager.INSTANCE.getDatatype(metadataProvider.getMetadataTxnContext(), dataverseName,
                    itemTypeName);
            IAType itemType = dt.getDatatype();
            ARecordType aRecordType = (ARecordType) itemType;
            aRecordType.validateKeyFields(stmtCreateIndex.getFieldExprs(), stmtCreateIndex.getIndexType());

            if (idx != null) {
                if (stmtCreateIndex.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("An index with this name " + indexName + " already exists.");
                }
            }

            List<FeedConnectionId> activeFeedConnections = FeedLifecycleListener.INSTANCE
                    .getActiveFeedConnections(null);
            boolean resourceInUse = false;
            if (activeFeedConnections != null && !activeFeedConnections.isEmpty()) {
                StringBuilder builder = new StringBuilder();
                for (FeedConnectionId connId : activeFeedConnections) {
                    if (connId.getDatasetName().equals(datasetName)) {
                        resourceInUse = true;
                        builder.append(connId + "\n");
                    }
                }
                if (resourceInUse) {
                    throw new AsterixException("Dataset" + datasetName
                            + " is currently being fed into by the following feeds " + "." + builder.toString()
                            + "\nOperation not supported.");
                }
            }

            //#. add a new index with PendingAddOp
            Index index = new Index(dataverseName, datasetName, indexName, stmtCreateIndex.getIndexType(),
                    stmtCreateIndex.getFieldExprs(), stmtCreateIndex.getGramLength(), false,
                    IMetadataEntity.PENDING_ADD_OP);
            MetadataManager.INSTANCE.addIndex(metadataProvider.getMetadataTxnContext(), index);

            //#. prepare to create the index artifact in NC.
            CompiledCreateIndexStatement cis = new CompiledCreateIndexStatement(index.getIndexName(), dataverseName,
                    index.getDatasetName(), index.getKeyFieldNames(), index.getGramLength(), index.getIndexType());
            spec = IndexOperations.buildSecondaryIndexCreationJobSpec(cis, metadataProvider);
            if (spec == null) {
                throw new AsterixException("Failed to create job spec for creating index '"
                        + stmtCreateIndex.getDatasetName() + "." + stmtCreateIndex.getIndexName() + "'");
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

            //#. create the index artifact in NC.
            runJob(hcc, spec, true);

            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);

            //#. load data into the index in NC.
            cis = new CompiledCreateIndexStatement(index.getIndexName(), dataverseName, index.getDatasetName(),
                    index.getKeyFieldNames(), index.getGramLength(), index.getIndexType());
            spec = IndexOperations.buildSecondaryIndexLoadingJobSpec(cis, metadataProvider);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            runJob(hcc, spec, true);

            //#. begin new metadataTxn
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);

            //#. add another new index with PendingNoOp after deleting the index with PendingAddOp
            MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), dataverseName, datasetName,
                    indexName);
            index.setPendingOp(IMetadataEntity.PENDING_NO_OP);
            MetadataManager.INSTANCE.addIndex(metadataProvider.getMetadataTxnContext(), index);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);

        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }

            if (progress == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                //#. execute compensation operations
                //   remove the index in NC
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                CompiledIndexDropStatement cds = new CompiledIndexDropStatement(dataverseName, datasetName, indexName);
                try {
                    JobSpecification jobSpec = IndexOperations
                            .buildDropSecondaryIndexJobSpec(cds, metadataProvider, ds);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    bActiveTxn = false;

                    runJob(hcc, jobSpec, true);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    if (bActiveTxn) {
                        abort(e, e2, mdTxnCtx);
                    }
                }

                //   remove the record from the metadata.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                try {
                    MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), dataverseName,
                            datasetName, indexName);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    throw new IllegalStateException("System is inconsistent state: pending index(" + dataverseName
                            + "." + datasetName + "." + indexName + ") couldn't be removed from the metadata", e);
                }
            }
            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private void handleCreateTypeStatement(AqlMetadataProvider metadataProvider, Statement stmt) throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        try {
            TypeDecl stmtCreateType = (TypeDecl) stmt;
            String dataverseName = getActiveDataverse(stmtCreateType.getDataverseName());
            String typeName = stmtCreateType.getIdent().getValue();
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverseName);
            if (dv == null) {
                throw new AlgebricksException("Unknown dataverse " + dataverseName);
            }
            Datatype dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataverseName, typeName);
            if (dt != null) {
                if (!stmtCreateType.getIfNotExists()) {
                    throw new AlgebricksException("A datatype with this name " + typeName + " already exists.");
                }
            } else {
                if (builtinTypeMap.get(typeName) != null) {
                    throw new AlgebricksException("Cannot redefine builtin type " + typeName + ".");
                } else {
                    Map<TypeSignature, IAType> typeMap = TypeTranslator.computeTypes(mdTxnCtx, (TypeDecl) stmt,
                            dataverseName);
                    TypeSignature typeSignature = new TypeSignature(dataverseName, typeName);
                    IAType type = typeMap.get(typeSignature);
                    MetadataManager.INSTANCE.addDatatype(mdTxnCtx, new Datatype(dataverseName, typeName, type, false));
                }
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private void handleDataverseDropStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {

        ProgressState progress = ProgressState.NO_PROGRESS;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        String dataverseName = null;
        List<JobSpecification> jobsToExecute = new ArrayList<JobSpecification>();
        try {
            DataverseDropStatement stmtDelete = (DataverseDropStatement) stmt;
            dataverseName = stmtDelete.getDataverseName().getValue();

            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverseName);
            if (dv == null) {
                if (stmtDelete.getIfExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("There is no dataverse with this name " + dataverseName + ".");
                }
            }

            //# disconnect all feeds from any datasets in the dataverse.
            List<FeedConnectionId> activeFeedConnections = FeedLifecycleListener.INSTANCE
                    .getActiveFeedConnections(null);
            DisconnectFeedStatement disStmt = null;
            Identifier dvId = new Identifier(dataverseName);
            for (FeedConnectionId connection : activeFeedConnections) {
                if (connection.getFeedId().getDataverse().equals(dataverseName)) {
                    disStmt = new DisconnectFeedStatement(dvId, new Identifier(connection.getFeedId().getFeedName()),
                            new Identifier(connection.getDatasetName()));
                    try {
                        handleDisconnectFeedStatement(metadataProvider, disStmt, hcc);
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("Disconnected feed " + connection.getFeedId().getFeedName() + " from dataset "
                                    + connection.getDatasetName());
                        }
                    } catch (Exception exception) {
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Unable to disconnect feed " + connection.getFeedId().getFeedName()
                                    + " from dataset " + connection.getDatasetName() + ". Encountered exception "
                                    + exception);
                        }
                    }
                }
            }

            //#. prepare jobs which will drop corresponding datasets with indexes. 
            List<Dataset> datasets = MetadataManager.INSTANCE.getDataverseDatasets(mdTxnCtx, dataverseName);
            for (int j = 0; j < datasets.size(); j++) {
                String datasetName = datasets.get(j).getDatasetName();
                DatasetType dsType = datasets.get(j).getDatasetType();
                if (dsType == DatasetType.INTERNAL) {

                    List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName,
                            datasetName);
                    for (int k = 0; k < indexes.size(); k++) {
                        if (indexes.get(k).isSecondaryIndex()) {
                            CompiledIndexDropStatement cds = new CompiledIndexDropStatement(dataverseName, datasetName,
                                    indexes.get(k).getIndexName());
                            jobsToExecute.add(IndexOperations.buildDropSecondaryIndexJobSpec(cds, metadataProvider,
                                    datasets.get(j)));
                        }
                    }

                    CompiledDatasetDropStatement cds = new CompiledDatasetDropStatement(dataverseName, datasetName);
                    jobsToExecute.add(DatasetOperations.createDropDatasetJobSpec(cds, metadataProvider));
                }
            }
            jobsToExecute.add(DataverseOperations.createDropDataverseJobSpec(dv, metadataProvider));

            //#. mark PendingDropOp on the dataverse record by 
            //   first, deleting the dataverse record from the DATAVERSE_DATASET
            //   second, inserting the dataverse record with the PendingDropOp value into the DATAVERSE_DATASET
            MetadataManager.INSTANCE.dropDataverse(mdTxnCtx, dataverseName);
            MetadataManager.INSTANCE.addDataverse(mdTxnCtx, new Dataverse(dataverseName, dv.getDataFormat(),
                    IMetadataEntity.PENDING_DROP_OP));

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

            for (JobSpecification jobSpec : jobsToExecute) {
                runJob(hcc, jobSpec, true);
            }

            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);

            //#. finally, delete the dataverse.
            MetadataManager.INSTANCE.dropDataverse(mdTxnCtx, dataverseName);
            if (activeDefaultDataverse != null && activeDefaultDataverse.getDataverseName() == dataverseName) {
                activeDefaultDataverse = null;
            }

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }

            if (progress == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                if (activeDefaultDataverse != null && activeDefaultDataverse.getDataverseName() == dataverseName) {
                    activeDefaultDataverse = null;
                }

                //#. execute compensation operations
                //   remove the all indexes in NC
                try {
                    for (JobSpecification jobSpec : jobsToExecute) {
                        runJob(hcc, jobSpec, true);
                    }
                } catch (Exception e2) {
                    //do no throw exception since still the metadata needs to be compensated. 
                    e.addSuppressed(e2);
                }

                //   remove the record from the metadata.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                try {
                    MetadataManager.INSTANCE.dropDataverse(mdTxnCtx, dataverseName);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    throw new IllegalStateException("System is inconsistent state: pending dataverse(" + dataverseName
                            + ") couldn't be removed from the metadata", e);
                }
            }

            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private void handleDatasetDropStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {

        ProgressState progress = ProgressState.NO_PROGRESS;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        String dataverseName = null;
        String datasetName = null;
        List<JobSpecification> jobsToExecute = new ArrayList<JobSpecification>();
        try {
            DropStatement stmtDelete = (DropStatement) stmt;
            dataverseName = getActiveDataverse(stmtDelete.getDataverseName());
            datasetName = stmtDelete.getDatasetName().getValue();

            Dataset ds = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
            if (ds == null) {
                if (stmtDelete.getIfExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("There is no dataset with this name " + datasetName
                            + " in dataverse " + dataverseName + ".");
                }
            }

            Map<FeedConnectionId, Pair<JobSpecification, Boolean>> disconnectJobList = new HashMap<FeedConnectionId, Pair<JobSpecification, Boolean>>();
            if (ds.getDatasetType() == DatasetType.INTERNAL) {
                // prepare job spec(s) that would disconnect any active feeds involving the dataset.
                List<FeedConnectionId> feedConnections = FeedLifecycleListener.INSTANCE.getActiveFeedConnections(null);
                if (feedConnections != null && !feedConnections.isEmpty()) {
                    for (FeedConnectionId connection : feedConnections) {
                        Pair<JobSpecification, Boolean> p = FeedOperations.buildDisconnectFeedJobSpec(metadataProvider,
                                connection);
                        disconnectJobList.put(connection, p);
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("Disconnecting feed " + connection.getFeedId().getFeedName() + " from dataset "
                                    + datasetName + " as dataset is being dropped");
                        }
                    }
                }

                //#. prepare jobs to drop the datatset and the indexes in NC
                List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName, datasetName);
                for (int j = 0; j < indexes.size(); j++) {
                    if (indexes.get(j).isSecondaryIndex()) {
                        CompiledIndexDropStatement cds = new CompiledIndexDropStatement(dataverseName, datasetName,
                                indexes.get(j).getIndexName());
                        jobsToExecute.add(IndexOperations.buildDropSecondaryIndexJobSpec(cds, metadataProvider, ds));
                    }
                }
                CompiledDatasetDropStatement cds = new CompiledDatasetDropStatement(dataverseName, datasetName);
                jobsToExecute.add(DatasetOperations.createDropDatasetJobSpec(cds, metadataProvider));

                //#. mark the existing dataset as PendingDropOp
                MetadataManager.INSTANCE.dropDataset(mdTxnCtx, dataverseName, datasetName);
                MetadataManager.INSTANCE.addDataset(
                        mdTxnCtx,
                        new Dataset(dataverseName, datasetName, ds.getItemTypeName(), ds.getDatasetDetails(), ds
                                .getHints(), ds.getDatasetType(), ds.getDatasetId(), IMetadataEntity.PENDING_DROP_OP));

                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                bActiveTxn = false;
                progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

                //# disconnect the feeds
                for (Pair<JobSpecification, Boolean> p : disconnectJobList.values()) {
                    runJob(hcc, p.first, true);
                }

                //#. run the jobs
                for (JobSpecification jobSpec : jobsToExecute) {
                    runJob(hcc, jobSpec, true);
                }

                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                for (Entry<FeedConnectionId, Pair<JobSpecification, Boolean>> entry : disconnectJobList.entrySet()) {
                    if (!entry.getValue().second) {
                        MetadataManager.INSTANCE.deregisterFeedActivity(mdTxnCtx, entry.getKey());
                    }
                }

                for (Entry<FeedConnectionId, Pair<JobSpecification, Boolean>> entry : disconnectJobList.entrySet()) {
                    if (!entry.getValue().second) {
                        MetadataManager.INSTANCE.deregisterFeedActivity(mdTxnCtx, entry.getKey());
                    }
                }
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
            }

            //#. finally, delete the dataset.
            MetadataManager.INSTANCE.dropDataset(mdTxnCtx, dataverseName, datasetName);
            // Drop the associated nodegroup
            if (ds.getDatasetType() == DatasetType.INTERNAL) {
                String nodegroup = ((InternalDatasetDetails) ds.getDatasetDetails()).getNodeGroupName();
                if (!nodegroup.equalsIgnoreCase(MetadataConstants.METADATA_DEFAULT_NODEGROUP_NAME)) {
                    MetadataManager.INSTANCE.dropNodegroup(mdTxnCtx, dataverseName + ":" + datasetName);
                }
            }

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }

            if (progress == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                //#. execute compensation operations
                //   remove the all indexes in NC
                try {
                    for (JobSpecification jobSpec : jobsToExecute) {
                        runJob(hcc, jobSpec, true);
                    }
                } catch (Exception e2) {
                    //do no throw exception since still the metadata needs to be compensated. 
                    e.addSuppressed(e2);
                }

                //   remove the record from the metadata.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                try {
                    MetadataManager.INSTANCE.dropDataset(metadataProvider.getMetadataTxnContext(), dataverseName,
                            datasetName);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    throw new IllegalStateException("System is inconsistent state: pending dataset(" + dataverseName
                            + "." + datasetName + ") couldn't be removed from the metadata", e);
                }
            }

            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private void handleIndexDropStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {

        ProgressState progress = ProgressState.NO_PROGRESS;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        String dataverseName = null;
        String datasetName = null;
        String indexName = null;
        List<JobSpecification> jobsToExecute = new ArrayList<JobSpecification>();
        try {
            IndexDropStatement stmtIndexDrop = (IndexDropStatement) stmt;
            datasetName = stmtIndexDrop.getDatasetName().getValue();
            dataverseName = getActiveDataverse(stmtIndexDrop.getDataverseName());

            Dataset ds = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
            if (ds == null) {
                throw new AlgebricksException("There is no dataset with this name " + datasetName + " in dataverse "
                        + dataverseName);
            }

            List<FeedConnectionId> feedConnections = FeedLifecycleListener.INSTANCE.getActiveFeedConnections(null);
            boolean resourceInUse = false;
            if (feedConnections != null && !feedConnections.isEmpty()) {
                StringBuilder builder = new StringBuilder();
                for (FeedConnectionId connection : feedConnections) {
                    if (connection.getDatasetName().equals(datasetName)) {
                        resourceInUse = true;
                        builder.append(connection + "\n");
                    }
                }
                if (resourceInUse) {
                    throw new AsterixException("Dataset" + datasetName
                            + " is currently being fed into by the following feeds " + "." + builder.toString()
                            + "\nOperation not supported.");
                }
            }

            if (ds.getDatasetType() == DatasetType.INTERNAL) {
                indexName = stmtIndexDrop.getIndexName().getValue();
                Index index = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataverseName, datasetName, indexName);
                if (index == null) {
                    if (stmtIndexDrop.getIfExists()) {
                        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                        return;
                    } else {
                        throw new AlgebricksException("There is no index with this name " + indexName + ".");
                    }
                }
                //#. prepare a job to drop the index in NC.
                CompiledIndexDropStatement cds = new CompiledIndexDropStatement(dataverseName, datasetName, indexName);
                jobsToExecute.add(IndexOperations.buildDropSecondaryIndexJobSpec(cds, metadataProvider, ds));

                //#. mark PendingDropOp on the existing index
                MetadataManager.INSTANCE.dropIndex(mdTxnCtx, dataverseName, datasetName, indexName);
                MetadataManager.INSTANCE.addIndex(mdTxnCtx,
                        new Index(dataverseName, datasetName, indexName, index.getIndexType(),
                                index.getKeyFieldNames(), index.isPrimaryIndex(), IMetadataEntity.PENDING_DROP_OP));

                //#. commit the existing transaction before calling runJob. 
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                bActiveTxn = false;
                progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

                for (JobSpecification jobSpec : jobsToExecute) {
                    runJob(hcc, jobSpec, true);
                }

                //#. begin a new transaction
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);

                //#. finally, delete the existing index
                MetadataManager.INSTANCE.dropIndex(mdTxnCtx, dataverseName, datasetName, indexName);
            } else {
                throw new AlgebricksException(datasetName
                        + " is an external dataset. Indexes are not maintained for external datasets.");
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);

        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }

            if (progress == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                //#. execute compensation operations
                //   remove the all indexes in NC
                try {
                    for (JobSpecification jobSpec : jobsToExecute) {
                        runJob(hcc, jobSpec, true);
                    }
                } catch (Exception e2) {
                    //do no throw exception since still the metadata needs to be compensated.
                    e.addSuppressed(e2);
                }

                //   remove the record from the metadata.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                try {
                    MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), dataverseName,
                            datasetName, indexName);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    throw new IllegalStateException("System is inconsistent state: pending index(" + dataverseName
                            + "." + datasetName + "." + indexName + ") couldn't be removed from the metadata", e);
                }
            }

            throw e;

        } finally {
            releaseWriteLatch();
        }
    }

    private void handleTypeDropStatement(AqlMetadataProvider metadataProvider, Statement stmt) throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        try {
            TypeDropStatement stmtTypeDrop = (TypeDropStatement) stmt;
            String dataverseName = getActiveDataverse(stmtTypeDrop.getDataverseName());
            String typeName = stmtTypeDrop.getTypeName().getValue();
            Datatype dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataverseName, typeName);
            if (dt == null) {
                if (!stmtTypeDrop.getIfExists())
                    throw new AlgebricksException("There is no datatype with this name " + typeName + ".");
            } else {
                MetadataManager.INSTANCE.dropDatatype(mdTxnCtx, dataverseName, typeName);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private void handleNodegroupDropStatement(AqlMetadataProvider metadataProvider, Statement stmt) throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        try {
            NodeGroupDropStatement stmtDelete = (NodeGroupDropStatement) stmt;
            String nodegroupName = stmtDelete.getNodeGroupName().getValue();
            NodeGroup ng = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, nodegroupName);
            if (ng == null) {
                if (!stmtDelete.getIfExists())
                    throw new AlgebricksException("There is no nodegroup with this name " + nodegroupName + ".");
            } else {
                MetadataManager.INSTANCE.dropNodegroup(mdTxnCtx, nodegroupName);
            }

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private void handleCreateFunctionStatement(AqlMetadataProvider metadataProvider, Statement stmt) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        try {
            CreateFunctionStatement cfs = (CreateFunctionStatement) stmt;
            String dataverse = getActiveDataverse(new Identifier(cfs.getSignature().getNamespace()));
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverse);
            if (dv == null) {
                throw new AlgebricksException("There is no dataverse with this name " + dataverse + ".");
            }

            Function function = MetadataManager.INSTANCE.getFunction(mdTxnCtx, cfs.getaAterixFunction());
            if (function != null) {
                throw new AsterixException("A function by the name " + cfs.getaAterixFunction().getName()
                        + " and arity " + cfs.getaAterixFunction().getArity() + " already exists in dataverse "
                        + dataverse);
            }
            function = new Function(dataverse, cfs.getaAterixFunction().getName(), cfs.getaAterixFunction().getArity(),
                    cfs.getParamList(), Function.RETURNTYPE_VOID, cfs.getFunctionBody(), Function.LANGUAGE_AQL,
                    FunctionKind.SCALAR.toString());
            MetadataManager.INSTANCE.addFunction(mdTxnCtx, function);

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private void handleFunctionDropStatement(AqlMetadataProvider metadataProvider, Statement stmt) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        try {
            FunctionDropStatement stmtDropFunction = (FunctionDropStatement) stmt;
            FunctionSignature signature = stmtDropFunction.getFunctionSignature();
            Function function = MetadataManager.INSTANCE.getFunction(mdTxnCtx, signature);
            if (function == null) {
                if (!stmtDropFunction.getIfExists())
                    throw new AlgebricksException("Unknonw function " + signature);
            } else {
                MetadataManager.INSTANCE.dropFunction(mdTxnCtx, signature);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private void handleLoadStatement(AqlMetadataProvider metadataProvider, Statement stmt, IHyracksClientConnection hcc)
            throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireReadLatch();
        List<JobSpecification> jobsToExecute = new ArrayList<JobSpecification>();
        try {
            LoadStatement loadStmt = (LoadStatement) stmt;
            String dataverseName = getActiveDataverse(loadStmt.getDataverseName());
            CompiledLoadFromFileStatement cls = new CompiledLoadFromFileStatement(dataverseName, loadStmt
                    .getDatasetName().getValue(), loadStmt.getAdapter(), loadStmt.getProperties(),
                    loadStmt.dataIsAlreadySorted());

            IDataFormat format = getDataFormat(metadataProvider.getMetadataTxnContext(), dataverseName);
            Job job = DatasetOperations.createLoadDatasetJobSpec(metadataProvider, cls, format);
            jobsToExecute.add(job.getJobSpec());
            // Also load the dataset's secondary indexes.
            List<Index> datasetIndexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName, loadStmt
                    .getDatasetName().getValue());
            for (Index index : datasetIndexes) {
                if (!index.isSecondaryIndex()) {
                    continue;
                }
                // Create CompiledCreateIndexStatement from metadata entity 'index'.
                CompiledCreateIndexStatement cis = new CompiledCreateIndexStatement(index.getIndexName(),
                        dataverseName, index.getDatasetName(), index.getKeyFieldNames(), index.getGramLength(),
                        index.getIndexType());
                jobsToExecute.add(IndexOperations.buildSecondaryIndexLoadingJobSpec(cis, metadataProvider));
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            for (JobSpecification jobspec : jobsToExecute) {
                runJob(hcc, jobspec, true);
            }
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }

            throw e;
        } finally {
            releaseReadLatch();
        }
    }

    private void handleInsertStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireReadLatch();

        try {
            metadataProvider.setWriteTransaction(true);
            InsertStatement stmtInsert = (InsertStatement) stmt;
            String dataverseName = getActiveDataverse(stmtInsert.getDataverseName());
            CompiledInsertStatement clfrqs = new CompiledInsertStatement(dataverseName, stmtInsert.getDatasetName()
                    .getValue(), stmtInsert.getQuery(), stmtInsert.getVarCounter());
            JobSpecification compiled = rewriteCompileQuery(metadataProvider, clfrqs.getQuery(), clfrqs);

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            if (compiled != null) {
                runJob(hcc, compiled, true);
            }

        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            releaseReadLatch();
        }
    }

    private void handleDeleteStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireReadLatch();

        try {
            metadataProvider.setWriteTransaction(true);
            DeleteStatement stmtDelete = (DeleteStatement) stmt;
            String dataverseName = getActiveDataverse(stmtDelete.getDataverseName());
            CompiledDeleteStatement clfrqs = new CompiledDeleteStatement(stmtDelete.getVariableExpr(), dataverseName,
                    stmtDelete.getDatasetName().getValue(), stmtDelete.getCondition(), stmtDelete.getVarCounter(),
                    metadataProvider);
            JobSpecification compiled = rewriteCompileQuery(metadataProvider, clfrqs.getQuery(), clfrqs);

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            if (compiled != null) {
                runJob(hcc, compiled, true);
            }

        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            releaseReadLatch();
        }
    }

    private JobSpecification rewriteCompileQuery(AqlMetadataProvider metadataProvider, Query query,
            ICompiledDmlStatement stmt) throws AsterixException, RemoteException, AlgebricksException, JSONException,
            ACIDException {

        // Query Rewriting (happens under the same ongoing metadata transaction)
        Pair<Query, Integer> reWrittenQuery = APIFramework.reWriteQuery(declaredFunctions, metadataProvider, query,
                sessionConfig, out, pdf);

        // Query Compilation (happens under the same ongoing metadata
        // transaction)
        JobSpecification spec = APIFramework.compileQuery(declaredFunctions, metadataProvider, reWrittenQuery.first,
                reWrittenQuery.second, stmt == null ? null : stmt.getDatasetName(), sessionConfig, out, pdf, stmt);

        return spec;

    }

    private void handleCreateFeedStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        String dataverseName = null;
        String feedName = null;
        Feed feed = null;
        try {
            CreateFeedStatement cfs = (CreateFeedStatement) stmt;
            dataverseName = getActiveDataverse(cfs.getDataverseName());
            feedName = cfs.getFeedName().getValue();

            feed = MetadataManager.INSTANCE.getFeed(metadataProvider.getMetadataTxnContext(), dataverseName, feedName);
            if (feed != null) {
                if (cfs.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("A feed with this name " + feedName + " already exists.");
                }
            }

            switch (stmt.getKind()) {
                case CREATE_PRIMARY_FEED:
                    CreatePrimaryFeedStatement cpfs = (CreatePrimaryFeedStatement) stmt;
                    String adaptorName = cpfs.getAdaptorName();
                    feed = new PrimaryFeed(dataverseName, feedName, adaptorName, cpfs.getAdaptorConfiguration(),
                            cfs.getAppliedFunction());
                    break;
                case CREATE_SECONDARY_FEED:
                    CreateSecondaryFeedStatement csfs = (CreateSecondaryFeedStatement) stmt;
                    feed = new SecondaryFeed(dataverseName, feedName, csfs.getSourceFeedName(),
                            csfs.getAppliedFunction());
                    break;
                default:
                    throw new IllegalStateException();
            }

            MetadataManager.INSTANCE.addFeed(metadataProvider.getMetadataTxnContext(), feed);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private void handleCreateFeedPolicyStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();
        String dataverse;
        String policy;
        FeedPolicy newPolicy = null;
        try {
            CreateFeedPolicyStatement cfps = (CreateFeedPolicyStatement) stmt;
            dataverse = getActiveDataverse(null);
            policy = cfps.getPolicyName();
            FeedPolicy feedPolicy = MetadataManager.INSTANCE.getFeedPolicy(metadataProvider.getMetadataTxnContext(),
                    dataverse, policy);
            if (feedPolicy != null) {
                if (cfps.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("A policy with this name " + policy + " already exists.");
                }
            }
            boolean extendingExisting = cfps.getSourcePolicyName() != null;
            String description = cfps.getDescription() == null ? "" : cfps.getDescription();
            if (extendingExisting) {
                FeedPolicy sourceFeedPolicy = MetadataManager.INSTANCE.getFeedPolicy(
                        metadataProvider.getMetadataTxnContext(), dataverse, cfps.getSourcePolicyName());
                if (sourceFeedPolicy == null) {
                    sourceFeedPolicy = MetadataManager.INSTANCE.getFeedPolicy(metadataProvider.getMetadataTxnContext(),
                            MetadataConstants.METADATA_DATAVERSE_NAME, cfps.getSourcePolicyName());
                    if (sourceFeedPolicy == null) {
                        throw new AlgebricksException("Unknown policy " + cfps.getSourcePolicyName());
                    }
                }
                Map<String, String> policyProperties = sourceFeedPolicy.getProperties();
                policyProperties.putAll(cfps.getProperties());
                newPolicy = new FeedPolicy(dataverse, policy, description, policyProperties);
            } else {
                Properties prop = new Properties();
                try {
                    InputStream stream = new FileInputStream(cfps.getSourcePolicyFile());
                    prop.load(stream);
                } catch (Exception e) {
                    throw new AlgebricksException("Unable to read policy file" + cfps.getSourcePolicyFile());
                }
                Map<String, String> policyProperties = new HashMap<String, String>();
                for (Entry<Object, Object> entry : prop.entrySet()) {
                    policyProperties.put((String) entry.getKey(), (String) entry.getValue());
                }
                newPolicy = new FeedPolicy(dataverse, policy, description, policyProperties);
            }
            MetadataManager.INSTANCE.addFeedPolicy(mdTxnCtx, newPolicy);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private void handleDropFeedStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        try {
            FeedDropStatement stmtFeedDrop = (FeedDropStatement) stmt;
            String dataverseName = getActiveDataverse(stmtFeedDrop.getDataverseName());
            String feedName = stmtFeedDrop.getFeedName().getValue();
            Feed feed = MetadataManager.INSTANCE.getFeed(mdTxnCtx, dataverseName, feedName);
            if (feed == null) {
                if (!stmtFeedDrop.getIfExists()) {
                    throw new AlgebricksException("Unknown feed " + feedName + ".");
                }
            }

            FeedId feedId = new FeedId(dataverseName, feedName);
            List<FeedConnectionId> activeConnections = FeedLifecycleListener.INSTANCE.getActiveFeedConnections(feedId);
            if (activeConnections != null && !activeConnections.isEmpty()) {
                StringBuilder builder = new StringBuilder();
                for (FeedConnectionId connectionId : activeConnections) {
                    builder.append(connectionId.getDatasetName() + "\n");
                }

                throw new AlgebricksException("Feed " + feedId
                        + " is currently active and connected to the following dataset(s) \n" + builder.toString());
            } else {
                MetadataManager.INSTANCE.dropFeed(mdTxnCtx, dataverseName, feedName);
            }

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Removed feed " + feedId);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);

        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private void handleDropFeedPolicyStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        try {
            FeedPolicyDropStatement stmtFeedPolicyDrop = (FeedPolicyDropStatement) stmt;
            String dataverseName = getActiveDataverse(stmtFeedPolicyDrop.getDataverseName());
            String policyName = stmtFeedPolicyDrop.getPolicyName().getValue();
            FeedPolicy feedPolicy = MetadataManager.INSTANCE.getFeedPolicy(mdTxnCtx, dataverseName, policyName);
            if (feedPolicy == null) {
                if (!stmtFeedPolicyDrop.getIfExists()) {
                    throw new AlgebricksException("Unknown policy " + policyName + " in dataverse " + dataverseName);
                }
            }
            MetadataManager.INSTANCE.dropFeedPolicy(mdTxnCtx, dataverseName, policyName);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private void handleConnectFeedStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireReadLatch();
        boolean readLatchAcquired = true;
        try {
            ConnectFeedStatement cfs = (ConnectFeedStatement) stmt;
            String dataverseName = getActiveDataverse(cfs.getDataverseName());
            metadataProvider.setWriteTransaction(true);

            CompiledConnectFeedStatement cbfs = new CompiledConnectFeedStatement(dataverseName, cfs.getFeedName(), cfs
                    .getDatasetName().getValue(), cfs.getPolicy(), cfs.getQuery(), cfs.getVarCounter());

            FeedUtil.validateIfDatasetExists(dataverseName, cfs.getDatasetName().getValue(),
                    metadataProvider.getMetadataTxnContext());

            Feed feed = FeedUtil.validateIfFeedExists(dataverseName, cfs.getFeedName(),
                    metadataProvider.getMetadataTxnContext());

            FeedConnectionId feedConnId = new FeedConnectionId(dataverseName, cfs.getFeedName(), cfs.getDatasetName()
                    .getValue());

            if (FeedLifecycleListener.INSTANCE.isFeedConnectionActive(feedConnId)) {
                throw new AsterixException("Feed " + cfs.getFeedName() + " is already connected to dataset "
                        + cfs.getDatasetName().getValue());
            }

            FeedPolicy feedPolicy = FeedUtil.validateIfPolicyExists(dataverseName, cbfs.getPolicyName(), mdTxnCtx);

            // All Metadata checks have passed. Feed connect request is valid. //

            FeedPolicyAccessor policyAccessor = new FeedPolicyAccessor(feedPolicy.getProperties());
            Pair<FeedConnectionRequest, Boolean> p = getFeedConnectionRequest(dataverseName, feed,
                    cbfs.getDatasetName(), feedPolicy, mdTxnCtx);
            FeedConnectionRequest connectionRequest = p.first;
            boolean createFeedIntakeJob = p.second;
            if (createFeedIntakeJob) {
                FeedId feedId = connectionRequest.getFeedJointKey().getFeedId();
                PrimaryFeed primaryFeed = (PrimaryFeed) MetadataManager.INSTANCE.getFeed(mdTxnCtx,
                        feedId.getDataverse(), feedId.getFeedName());
                Pair<JobSpecification, IFeedAdapterFactory> pair = FeedOperations.buildFeedIntakeJobSpec(primaryFeed,
                        metadataProvider, policyAccessor);
                runJob(hcc, pair.first, false);
                IFeedAdapterFactory adapterFactory = pair.second;
                if (adapterFactory.isRecordTrackingEnabled()) {
                    FeedLifecycleListener.INSTANCE.registerFeedIntakeProgressTracker(feedConnId,
                            adapterFactory.createIntakeProgressTracker());
                }
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            if (Boolean.valueOf(metadataProvider.getConfig().get(ConnectFeedStatement.WAIT_FOR_COMPLETION))) {
                IFeedLifecycleEventSubscriber eventSubscriber = new FeedLifecycleEventSubscriber();
                FeedLifecycleListener.INSTANCE.registerFeedEventSubscriber(feedConnId, eventSubscriber);
                releaseReadLatch();
                readLatchAcquired = false;
                eventSubscriber.waitForFeedCompletion(); // blocking call
            }
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            if (readLatchAcquired) {
                releaseReadLatch();
            }
        }
    }

    /**
     * Generates a subscription request corresponding to a connect feed request. In addition, provides a boolean
     * flag indicating if feed intake job needs to be started (source primary feed not found to be active).
     * 
     * @param dataverse
     * @param feed
     * @param dataset
     * @param feedPolicy
     * @param mdTxnCtx
     * @return
     * @throws MetadataException
     */
    private Pair<FeedConnectionRequest, Boolean> getFeedConnectionRequest(String dataverse, Feed feed, String dataset,
            FeedPolicy feedPolicy, MetadataTransactionContext mdTxnCtx) throws MetadataException {
        IFeedJoint sourceFeedJoint = null;
        FeedConnectionRequest request = null;
        List<String> functionsToApply = new ArrayList<String>();
        boolean needIntakeJob = false;
        List<IFeedJoint> jointsToRegister = new ArrayList<IFeedJoint>();
        FeedConnectionId connectionId = new FeedConnectionId(feed.getFeedId(), dataset);

        ConnectionLocation connectionLocation = null;
        FeedJointKey feedJointKey = getFeedJointKey(feed, mdTxnCtx);
        boolean isFeedJointAvailable = FeedLifecycleListener.INSTANCE.isFeedJointAvailable(feedJointKey);
        if (!isFeedJointAvailable) {
            sourceFeedJoint = FeedLifecycleListener.INSTANCE.getAvailableFeedJoint(feedJointKey);
            if (sourceFeedJoint == null) { // the feed is currently not being ingested, i.e., it is unavailable.
                connectionLocation = ConnectionLocation.SOURCE_FEED_INTAKE_STAGE;
                FeedId sourceFeedId = feedJointKey.getFeedId(); // the root/primary feedId 
                Feed primaryFeed = MetadataManager.INSTANCE.getFeed(mdTxnCtx, dataverse, sourceFeedId.getFeedName());
                FeedJointKey intakeFeedJointKey = new FeedJointKey(sourceFeedId, new ArrayList<String>());
                sourceFeedJoint = new FeedJoint(intakeFeedJointKey, primaryFeed.getFeedId(), connectionLocation,
                        FeedJointType.INTAKE, connectionId);
                jointsToRegister.add(sourceFeedJoint);
                needIntakeJob = true;
            } else {
                connectionLocation = sourceFeedJoint.getConnectionLocation();
            }

            String[] functions = feedJointKey.getStringRep()
                    .substring(sourceFeedJoint.getFeedJointKey().getStringRep().length()).trim().split(":");
            for (String f : functions) {
                if (f.trim().length() > 0) {
                    functionsToApply.add(f);
                }
            }
            // register the compute feed point that represents the final output from the collection of
            // functions that will be applied. 
            if (!functionsToApply.isEmpty()) {
                FeedJointKey computeFeedJointKey = new FeedJointKey(feed.getFeedId(), functionsToApply);
                IFeedJoint computeFeedJoint = new FeedJoint(computeFeedJointKey, feed.getFeedId(),
                        ConnectionLocation.SOURCE_FEED_COMPUTE_STAGE, FeedJointType.COMPUTE, connectionId);
                jointsToRegister.add(computeFeedJoint);
            }
            for (IFeedJoint joint : jointsToRegister) {
                FeedLifecycleListener.INSTANCE.registerFeedJoint(joint);
            }
        } else {
            sourceFeedJoint = FeedLifecycleListener.INSTANCE.getFeedJoint(feedJointKey);
            connectionLocation = sourceFeedJoint.getConnectionLocation();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Feed joint " + sourceFeedJoint + " is available! need not apply any further computation");
            }
        }

        request = new FeedConnectionRequest(sourceFeedJoint.getFeedJointKey(), connectionLocation, functionsToApply,
                dataset, feedPolicy.getPolicyName(), feedPolicy.getProperties(), feed.getFeedId());

        sourceFeedJoint.addConnectionRequest(request);
        return new Pair<FeedConnectionRequest, Boolean>(request, needIntakeJob);
    }

    /*
     * Gets the feed joint corresponding to the feed definition. Tuples constituting the feed are 
     * available at this feed joint.
     */
    private FeedJointKey getFeedJointKey(Feed feed, MetadataTransactionContext ctx) throws MetadataException {
        Feed sourceFeed = feed;
        List<String> appliedFunctions = new ArrayList<String>();
        while (sourceFeed.getFeedType().equals(FeedType.SECONDARY)) {
            if (sourceFeed.getAppliedFunction() != null) {
                appliedFunctions.add(0, sourceFeed.getAppliedFunction().getName());
            }
            Feed parentFeed = MetadataManager.INSTANCE.getFeed(ctx, feed.getDataverseName(),
                    ((SecondaryFeed) sourceFeed).getSourceFeedName());
            sourceFeed = parentFeed;
        }

        if (sourceFeed.getAppliedFunction() != null) {
            appliedFunctions.add(0, sourceFeed.getAppliedFunction().getName());
        }

        return new FeedJointKey(sourceFeed.getFeedId(), appliedFunctions);
    }

    private void handleDisconnectFeedStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        try {
            DisconnectFeedStatement cfs = (DisconnectFeedStatement) stmt;
            String dataverseName = getActiveDataverse(cfs.getDataverseName());

            FeedUtil.validateIfDatasetExists(dataverseName, cfs.getDatasetName().getValue(), mdTxnCtx);
            Feed feed = FeedUtil.validateIfFeedExists(dataverseName, cfs.getFeedName().getValue(), mdTxnCtx);

            FeedConnectionId connectionId = new FeedConnectionId(feed.getFeedId(), cfs.getDatasetName().getValue());
            boolean isFeedConnectionActive = FeedLifecycleListener.INSTANCE.isFeedConnectionActive(connectionId);
            if (!isFeedConnectionActive) {
                throw new AsterixException("Feed " + feed.getFeedId().getFeedName() + " is currently not connected to "
                        + cfs.getDatasetName().getValue() + ". Invalid operation!");
            }

            Pair<JobSpecification, Boolean> specDisconnectType = FeedOperations.buildDisconnectFeedJobSpec(
                    metadataProvider, connectionId);
            JobSpecification jobSpec = specDisconnectType.first;
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            runJob(hcc, jobSpec, true);

            if (!specDisconnectType.second) {
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                MetadataManager.INSTANCE.deregisterFeedActivity(mdTxnCtx, connectionId);
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                FeedLifecycleListener.INSTANCE.reportPartialDisconnection(connectionId);
            }

        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private void handleSubscribeFeedStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Subscriber Feed Statement :" + stmt);
        }

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireReadLatch();

        try {
            metadataProvider.setWriteTransaction(true);
            SubscribeFeedStatement bfs = (SubscribeFeedStatement) stmt;
            bfs.initialize(metadataProvider.getMetadataTxnContext());

            metadataProvider.getConfig().put(FunctionUtils.IMPORT_PRIVATE_FUNCTIONS, "" + Boolean.TRUE);
            metadataProvider.getConfig().put(FeedActivityDetails.FEED_POLICY_NAME, "" + bfs.getPolicy());
            metadataProvider.getConfig().put(FeedActivityDetails.COLLECT_LOCATIONS,
                    StringUtils.join(bfs.getLocations(), ','));

            CompiledSubscribeFeedStatement csfs = new CompiledSubscribeFeedStatement(bfs.getSubscriptionRequest(),
                    bfs.getQuery(), bfs.getVarCounter());
            JobSpecification compiled = rewriteCompileQuery(metadataProvider, bfs.getQuery(), csfs);
            FeedConnectionId feedConnectionId = new FeedConnectionId(bfs.getSubscriptionRequest().getReceivingFeedId(),
                    bfs.getSubscriptionRequest().getTargetDataset());

            JobSpecification alteredJobSpec = FeedUtil.alterJobSpecificationForFeed(compiled, feedConnectionId, bfs
                    .getSubscriptionRequest().getPolicyParameters());
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            if (compiled != null) {
                runJob(hcc, alteredJobSpec, false);
            }

        } catch (Exception e) {
            e.printStackTrace();
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            releaseReadLatch();
        }
    }

    private void handleCompactStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireReadLatch();

        String dataverseName = null;
        String datasetName = null;
        List<JobSpecification> jobsToExecute = new ArrayList<JobSpecification>();
        try {
            CompactStatement compactStatement = (CompactStatement) stmt;
            dataverseName = getActiveDataverse(compactStatement.getDataverseName());
            datasetName = compactStatement.getDatasetName().getValue();

            Dataset ds = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
            if (ds == null) {
                throw new AlgebricksException("There is no dataset with this name " + datasetName + " in dataverse "
                        + dataverseName + ".");
            } else if (ds.getDatasetType() != DatasetType.INTERNAL) {
                throw new AlgebricksException("Cannot compact the extrenal dataset " + datasetName + ".");
            }

            // Prepare jobs to compact the datatset and its indexes
            List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName, datasetName);
            for (int j = 0; j < indexes.size(); j++) {
                if (indexes.get(j).isSecondaryIndex()) {
                    CompiledIndexCompactStatement cics = new CompiledIndexCompactStatement(dataverseName, datasetName,
                            indexes.get(j).getIndexName(), indexes.get(j).getKeyFieldNames(), indexes.get(j)
                                    .getGramLength(), indexes.get(j).getIndexType());
                    jobsToExecute.add(IndexOperations.buildSecondaryIndexCompactJobSpec(cics, metadataProvider, ds));
                }
            }
            Dataverse dataverse = MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(),
                    dataverseName);
            jobsToExecute.add(DatasetOperations.compactDatasetJobSpec(dataverse, datasetName, metadataProvider));

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            //#. run the jobs
            for (JobSpecification jobSpec : jobsToExecute) {
                runJob(hcc, jobSpec, true);
            }
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            releaseReadLatch();
        }
    }

    private QueryResult handleQuery(AqlMetadataProvider metadataProvider, Query query, IHyracksClientConnection hcc,
            IHyracksDataset hdc, boolean asyncResults) throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireReadLatch();

        try {
            JobSpecification compiled = rewriteCompileQuery(metadataProvider, query, null);

            QueryResult queryResult = new QueryResult(query, metadataProvider.getResultSetId());
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            if (sessionConfig.isExecuteQuery() && compiled != null) {
                GlobalConfig.ASTERIX_LOGGER.info(compiled.toJSON().toString(1));
                JobId jobId = runJob(hcc, compiled, false);

                JSONObject response = new JSONObject();
                if (asyncResults) {
                    JSONArray handle = new JSONArray();
                    handle.put(jobId.getId());
                    handle.put(metadataProvider.getResultSetId().getId());
                    response.put("handle", handle);
                    out.print(response);
                    out.flush();
                } else {
                    if (pdf == DisplayFormat.HTML) {
                        out.println("<h4>Results:</h4>");
                        out.println("<pre>");
                    }

                    ByteBuffer buffer = ByteBuffer.allocate(ResultReader.FRAME_SIZE);
                    ResultReader resultReader = new ResultReader(hcc, hdc);
                    resultReader.open(jobId, metadataProvider.getResultSetId());
                    buffer.clear();

                    JSONArray results = new JSONArray();
                    while (resultReader.read(buffer) > 0) {
                        ResultUtils.getJSONFromBuffer(buffer, resultReader.getFrameTupleAccessor(), results);
                        buffer.clear();
                    }

                    response.put("results", results);
                    switch (pdf) {
                        case HTML:
                            ResultUtils.prettyPrintHTML(out, response);
                            break;
                        case TEXT:
                        case JSON:
                            out.print(response);
                            break;
                    }
                    out.flush();

                    if (pdf == DisplayFormat.HTML) {
                        out.println("</pre>");
                    }

                }
                hcc.waitForCompletion(jobId);
            }

            return queryResult;
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            releaseReadLatch();
        }
    }

    private void handleCreateNodeGroupStatement(AqlMetadataProvider metadataProvider, Statement stmt) throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        try {
            NodegroupDecl stmtCreateNodegroup = (NodegroupDecl) stmt;
            String ngName = stmtCreateNodegroup.getNodegroupName().getValue();
            NodeGroup ng = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, ngName);
            if (ng != null) {
                if (!stmtCreateNodegroup.getIfNotExists())
                    throw new AlgebricksException("A nodegroup with this name " + ngName + " already exists.");
            } else {
                List<Identifier> ncIdentifiers = stmtCreateNodegroup.getNodeControllerNames();
                List<String> ncNames = new ArrayList<String>(ncIdentifiers.size());
                for (Identifier id : ncIdentifiers) {
                    ncNames.add(id.getValue());
                }
                MetadataManager.INSTANCE.addNodegroup(mdTxnCtx, new NodeGroup(ngName, ncNames));
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private JobId runJob(IHyracksClientConnection hcc, JobSpecification spec, boolean waitForCompletion)
            throws Exception {
        spec.setFrameSize(AsterixAppContextInfo.getInstance().getCompilerProperties().getFrameSize());
        //OptimizationConfUtil.getPhysicalOptimizationConfig().getFrameSize());
        JobId[] jobIds = executeJobArray(hcc, new Job[] { new Job(spec) }, out, pdf, waitForCompletion);
        return jobIds[0];
    }

    public JobId[] executeJobArray(IHyracksClientConnection hcc, Job[] jobs, PrintWriter out, DisplayFormat pdf,
            boolean waitForCompletion) throws Exception {
        JobId[] startedJobIds = new JobId[jobs.length];
        for (int i = 0; i < jobs.length; i++) {
            JobSpecification spec = jobs[i].getJobSpec();
            spec.setMaxReattempts(0);
            JobId jobId = hcc.startJob(spec);
            startedJobIds[i] = jobId;
            if (waitForCompletion) {
                hcc.waitForCompletion(jobId);
            }
        }
        return startedJobIds;
    }

    private static IDataFormat getDataFormat(MetadataTransactionContext mdTxnCtx, String dataverseName)
            throws AsterixException {
        Dataverse dataverse = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverseName);
        IDataFormat format;
        try {
            format = (IDataFormat) Class.forName(dataverse.getDataFormat()).newInstance();
        } catch (Exception e) {
            throw new AsterixException(e);
        }
        return format;
    }

    private String getActiveDataverseName(String dataverse) throws AlgebricksException {
        if (dataverse != null) {
            return dataverse;
        }
        if (activeDefaultDataverse != null) {
            return activeDefaultDataverse.getDataverseName();
        }
        throw new AlgebricksException("dataverse not specified");
    }

    private String getActiveDataverse(Identifier dataverse) throws AlgebricksException {
        return getActiveDataverseName(dataverse != null ? dataverse.getValue() : null);
    }

    private void acquireWriteLatch() {
        MetadataManager.INSTANCE.acquireWriteLatch();
    }

    private void releaseWriteLatch() {
        MetadataManager.INSTANCE.releaseWriteLatch();
    }

    private void acquireReadLatch() {
        MetadataManager.INSTANCE.acquireReadLatch();
    }

    private void releaseReadLatch() {
        MetadataManager.INSTANCE.releaseReadLatch();
    }

    private void abort(Exception rootE, Exception parentE, MetadataTransactionContext mdTxnCtx) {
        try {
            if (IS_DEBUG_MODE) {
                rootE.printStackTrace();
            }
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
        } catch (Exception e2) {
            parentE.addSuppressed(e2);
            throw new IllegalStateException(rootE);
        }
    }
}
