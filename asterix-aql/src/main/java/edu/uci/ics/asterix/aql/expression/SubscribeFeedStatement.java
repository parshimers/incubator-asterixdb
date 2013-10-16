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
package edu.uci.ics.asterix.aql.expression;

import java.io.StringReader;
import java.util.List;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.aql.parser.AQLParser;
import edu.uci.ics.asterix.aql.parser.ParseException;
import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.entities.Feed;
import edu.uci.ics.asterix.metadata.entities.Function;
import edu.uci.ics.asterix.metadata.feeds.FeedSubscriptionRequest;

/**
 * Represents the AQL statement for subscribing to a feed.
 * This AQL statement is private and may not be used by the end-user.
 */
public class SubscribeFeedStatement implements Statement {

    private final FeedSubscriptionRequest subscriptionRequest;
    private Query query;
    private int varCounter;
    private boolean forceConnect = false;

    public static final String WAIT_FOR_COMPLETION = "wait-for-completion-feed";

    public SubscribeFeedStatement(FeedSubscriptionRequest subscriptionRequest) {
        this.subscriptionRequest = subscriptionRequest;
        this.varCounter = 0;
    }

    public void initialize(MetadataTransactionContext mdTxnCtx) throws MetadataException {
        query = new Query();
        FeedId sourceFeedId = subscriptionRequest.getSourceFeed().getFeedId();
        Feed subscriberFeed = MetadataManager.INSTANCE.getFeed(mdTxnCtx, subscriptionRequest.getFeed()
                .getDataverseName(), subscriptionRequest.getFeed().getFeedName());
        if (subscriberFeed == null) {
            throw new IllegalStateException(" Subsciber feed " + subscriberFeed + " not found.");
        }

        FunctionSignature appliedFunction = subscriberFeed.getAppliedFunction();
        Function function = null;
        String adapterOutputType = null;
        if (appliedFunction != null) {
            function = MetadataManager.INSTANCE.getFunction(mdTxnCtx, appliedFunction);
            if (function == null) {
                throw new MetadataException(" Unknown function " + function);
            } else if (function.getParams().size() > 1) {
                throw new MetadataException(" Incompatible function: " + appliedFunction
                        + " Number if arguments must be 1");
            }
        }

        StringBuilder builder = new StringBuilder();
        builder.append("set" + " " + FunctionUtils.IMPORT_PRIVATE_FUNCTIONS + " " + "'" + Boolean.TRUE + "'" + ";\n");
        builder.append("insert into dataset " + subscriptionRequest.getTargetDataset() + " ");

        if (appliedFunction == null) {
            builder.append(" (" + " for $x in feed-collect ('" + sourceFeedId.getDataverse() + "'" + "," + "'"
                    + sourceFeedId.getFeedName() + "'" + "," + "'" + subscriptionRequest.getFeed().getFeedName() + "'"
                    + "," + "'" + subscriptionRequest.getTargetDataset() + "'" + ")");
            builder.append(" return $x");
        } else {
            if (function.getLanguage().equalsIgnoreCase(Function.LANGUAGE_AQL)) {
                String param = function.getParams().get(0);
                builder.append(" (" + " for $x in feed-collect ('" + sourceFeedId.getDataverse() + "'" + "," + "'"
                        + sourceFeedId.getFeedName() + "'" + "," + "'" + subscriptionRequest.getFeed().getFeedName()
                        + "'" + "," + "'" + subscriptionRequest.getTargetDataset() + "'" + ")");
                builder.append(" let $y:=(" + function.getFunctionBody() + ")" + " return $y");
            } else {
                builder.append(" (" + " for $x in feed-collect ('" + sourceFeedId.getDataverse() + "'" + "," + "'"
                        + sourceFeedId.getFeedName() + "'" + "," + "'" + subscriptionRequest.getFeed().getFeedName()
                        + "'" + "," + "'" + subscriptionRequest.getTargetDataset() + "'" + ")");

                builder.append(" let $y:=" + function.getName() + "(" + "$x" + ")");
                builder.append(" return $y");
            }
        }
        builder.append(")");
        builder.append(";");
        AQLParser parser = new AQLParser(new StringReader(builder.toString()));

        List<Statement> statements;
        try {
            statements = parser.Statement();
            query = ((InsertStatement) statements.get(1)).getQuery();
        } catch (ParseException pe) {
            throw new MetadataException(pe);
        }

    }

    public Query getQuery() {
        return query;
    }

    public int getVarCounter() {
        return varCounter;
    }

    @Override
    public Kind getKind() {
        return Kind.SUBSCRIBE_FEED;
    }

    public String getPolicy() {
        return subscriptionRequest.getPolicy();
    }

    public FeedSubscriptionRequest getSubscriptionRequest() {
        return subscriptionRequest;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return null;
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
    }

    public String getDataverseName() {
        return subscriptionRequest.getSourceFeed().getDataverseName();
    }

}
