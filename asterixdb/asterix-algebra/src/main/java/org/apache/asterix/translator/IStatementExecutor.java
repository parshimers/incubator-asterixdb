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
package org.apache.asterix.translator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.asterix.common.api.IResponsePrinter;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.translator.CompiledStatements.ICompiledDmlStatement;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.client.IClusterInfoCollector;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.result.ResultSetId;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

/**
 * An interface that takes care of executing a list of statements that are submitted through an Asterix API
 */
public interface IStatementExecutor {
    char UNIT_SEPARATOR = 31;
    char END_OF_BLOCK = 23;

    /**
     * Specifies result delivery of executed statements
     */
    enum ResultDelivery {
        /**
         * Results are returned with the first response
         */
        IMMEDIATE("immediate"),
        /**
         * Results are produced completely, but only a result handle is returned
         */
        DEFERRED("deferred"),
        /**
         * A result handle is returned before the resutlts are complete
         */
        ASYNC("async");

        private static final Map<String, ResultDelivery> deliveryNames =
                Collections.unmodifiableMap(Arrays.stream(ResultDelivery.values())
                        .collect(Collectors.toMap(ResultDelivery::getName, Function.identity())));
        private final String name;

        ResultDelivery(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public static ResultDelivery fromName(String name) {
            return deliveryNames.get(name);
        }
    }

    class ResultMetadata implements Serializable {
        private static final long serialVersionUID = 1L;

        private final List<Triple<JobId, ResultSetId, ARecordType>> resultSets = new ArrayList<>();

        public List<Triple<JobId, ResultSetId, ARecordType>> getResultSets() {
            return resultSets;
        }

    }

    class Stats implements Serializable {
        private static final long serialVersionUID = 5885273238208454611L;

        public enum ProfileType {
            COUNTS("counts"),
            FULL("timings"),
            NONE("off");

            private static final Map<String, ProfileType> profileNames = Collections.unmodifiableMap(Arrays
                    .stream(ProfileType.values()).collect(Collectors.toMap(ProfileType::getName, Function.identity())));
            private final String name;

            ProfileType(String name) {
                this.name = name;
            }

            public String getName() {
                return name;
            }

            public static ProfileType fromName(String name) {
                return profileNames.get(name);
            }
        }

        private long count;
        private long size;
        private long processedObjects;
        private Profile profile;
        private ProfileType profileType;
        private long totalWarningsCount;
        private long compileTime;

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        public long getSize() {
            return size;
        }

        public void setSize(long size) {
            this.size = size;
        }

        public long getProcessedObjects() {
            return processedObjects;
        }

        public void setProcessedObjects(long processedObjects) {
            this.processedObjects = processedObjects;
        }

        public long getTotalWarningsCount() {
            return totalWarningsCount;
        }

        public void updateTotalWarningsCount(long delta) {
            if (delta <= Long.MAX_VALUE - totalWarningsCount) {
                totalWarningsCount += delta;
            }
        }

        public void setJobProfile(ObjectNode profile) {
            this.profile = new Profile(profile);
        }

        public ObjectNode getJobProfile() {
            return profile != null ? profile.getProfile() : null;
        }

        public ProfileType getProfileType() {
            return profileType;
        }

        public void setProfileType(ProfileType profileType) {
            this.profileType = profileType;
        }

        public void setCompileTime(long compileTime) {
            this.compileTime = compileTime;
        }

        public long getCompileTime() {
            return compileTime;
        }
    }

    class Profile implements Serializable {
        private static final long serialVersionUID = 4813321148252768375L;

        private transient ObjectNode profile;

        public Profile(ObjectNode profile) {
            this.profile = profile;
        }

        private void writeObject(ObjectOutputStream out) throws IOException {
            ObjectMapper om = new ObjectMapper();
            java.lang.String prof = om.writeValueAsString(profile);
            //split the string if it is >=64K to avoid writeUTF limit
            List<String> pieces;
            if (prof.length() > 65534L) {
                pieces = Lists.newArrayList(Splitter.fixedLength(32768).split(prof));
            } else {
                pieces = Lists.newArrayList(prof);
            }

            for (int i = 0; i < pieces.size(); i++) {
                out.writeChar(UNIT_SEPARATOR);
                out.writeUTF(pieces.get(i));
            }
            out.writeChar(END_OF_BLOCK);

        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            ObjectMapper om = new ObjectMapper();
            StringBuilder objSplits = new StringBuilder();
            char cmd = in.readChar();
            if (cmd == UNIT_SEPARATOR) {
                for (; cmd != END_OF_BLOCK && cmd == UNIT_SEPARATOR; cmd = in.readChar()) {
                    objSplits.append(in.readUTF());
                }
            } else {
                objSplits.append(cmd);
                objSplits.append(in.readUTF());
            }

            JsonNode inNode = om.readTree(objSplits.toString());
            if (!inNode.isObject()) {
                throw new IOException("Deserialization error");
            }
            profile = (ObjectNode) inNode;
        }

        public ObjectNode getProfile() {
            return profile;
        }
    }

    class StatementProperties implements Serializable {
        private static final long serialVersionUID = -1L;

        private Statement.Kind kind;
        private String name;

        public Statement.Kind getKind() {
            return kind;
        }

        public void setKind(Statement.Kind kind) {
            this.kind = kind;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public boolean isValid() {
            return kind != null && (kind != Statement.Kind.EXTENSION || name != null);
        }

        @Override
        public String toString() {
            return Statement.Kind.EXTENSION == kind ? String.valueOf(name) : String.valueOf(kind);
        }
    }

    /**
     * Compiles and executes a list of statements
     *
     * @param hcc
     * @param requestParameters
     * @return elapsed compilation time
     * @throws Exception
     */
    void compileAndExecute(IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception;

    /**
     * rewrites and compiles query into a hyracks job specifications
     *
     * @param clusterInfoCollector
     *            The cluster info collector
     * @param metadataProvider
     *            The metadataProvider used to access metadata and build runtimes
     * @param query
     *            The query to be compiled
     * @param dmlStatement
     *            The data modification statement when the query results in a modification to a dataset
     * @param statementParameters
     *            Statement parameters
     * @param requestParameters
     *            The request parameters
     * @return the compiled {@code JobSpecification}
     * @throws AsterixException
     * @throws RemoteException
     * @throws AlgebricksException
     * @throws ACIDException
     */
    JobSpecification rewriteCompileQuery(IClusterInfoCollector clusterInfoCollector, MetadataProvider metadataProvider,
            Query query, ICompiledDmlStatement dmlStatement, Map<String, IAObject> statementParameters,
            IRequestParameters requestParameters) throws RemoteException, AlgebricksException, ACIDException;

    /**
     * returns the active dataverse for an entity or a statement
     *
     * @param dataverseName:
     *            the entity or statement dataverse
     * @return
     *         returns the passed dataverse if not null, the active dataverse otherwise
     */
    DataverseName getActiveDataverseName(DataverseName dataverseName);

    /**
     * Gets the execution plans that are generated during query compilation
     *
     * @return the executions plans
     */
    ExecutionPlans getExecutionPlans();

    /**
     * Gets the response printer
     *
     * @return the responer printer
     */
    IResponsePrinter getResponsePrinter();

    /**
     * Gets the warnings generated during compiling and executing a request up to the max number argument.
     */
    void getWarnings(Collection<? super Warning> outWarnings, long maxWarnings);
}
