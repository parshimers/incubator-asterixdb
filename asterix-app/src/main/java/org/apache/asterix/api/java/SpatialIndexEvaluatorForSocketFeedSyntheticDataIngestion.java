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

package org.apache.asterix.api.java;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.http.HttpResponse;

import org.apache.hyracks.api.util.StopWatch;

public class SpatialIndexEvaluatorForSocketFeedSyntheticDataIngestion {
    private static String ccIpAddress = "127.0.0.1";
    private static String ccPortNum = "19002";
    private static String indexType;
    private static StopWatch sw = new StopWatch();
    private static String admFilePath;
    private static String socketAdapterIp;
    private static int socketAdapterPort;

    public static void main(String[] args) throws URISyntaxException, IOException {
        if (args.length < 6) {
            System.out
                    .println("Example Usage: java -jar SpatialIndexEvaluatorForSocketFeedSyntheticDataIngestion.jar <index type> <cc ip address> <asterix api port num> <socket_adater ip> <socket_adapter port> <adm file path>");
            System.out.println("\targ0: index type - rtree, shbtree, dhbtree, dhvbtree, or sif");
            System.out.println("\targ1: asterix cc ip address");
            System.out.println("\targ2: asterix api port number");
            System.out.println("\targ3: socket_adapter ip address");
            System.out.println("\targ4: socket_adapter port number");
            System.out.println("\targ5: adm file path");
            System.exit(-1);
        }
        indexType = args[0];
        ccIpAddress = args[1];
        ccPortNum = args[2];
        socketAdapterIp = args[3];
        socketAdapterPort = Integer.parseInt(args[4]);
        admFilePath = args[5];
        runFeed();
    }

    private static void runFeed() throws URISyntaxException, IOException {
        HttpResponse response;
        AsterixHttpClient ahc = new AsterixHttpClient(ccIpAddress, ccPortNum);
        StringBuilder sb = new StringBuilder();
        FileOutputStream fos = null;
        fos = ahc.openOutputFile("./" + indexType + "IngestionResult.txt");

        try {
//            // ddl
//            ahc.prepareDDL(getDDLAQL());
//            response = ahc.execute();
//            ahc.printResult(response, null);
//
//            // connect feed
//            ahc.prepareUpdate(getIngestionAQL());
//            response = ahc.execute();
//            ahc.printResult(response, fos);
            
            // ingest data using socket client
            runSocketCliet();
        } finally {
            ahc.closeOutputFile(fos);
            System.out.println(sb.toString());
        }
    }


    private static String getDDLAQL() {
        StringBuilder sb = new StringBuilder();
        //create dataverse
        sb.append("drop dataverse STBench if exists; \n");
        sb.append("create dataverse STBench; \n");
        sb.append("use dataverse STBench; \n");

        //create datatype
        sb.append(" create type FsqCheckinTweetType as closed { id: int64, user_id: int64, user_followers_count: int64, ");
        sb.append(" text: string, datetime: datetime, coordinates: point, url: string? } \n");

        //create datasets
        sb.append(" create dataset FsqCheckinTweet (FsqCheckinTweetType) primary key id \n");

        //create indexes
        if (indexType.contains("rtree") || indexType.contains("dhbtree") || indexType.contains("dhvbtree")) {
            sb.append("create index " + indexType + "CheckinCoordinate on FsqCheckinTweet(coordinates) type "
                    + indexType + " ;\n");
        } else {
            sb.append("create index " + indexType + "CheckinCoordinate on FsqCheckinTweet(coordinates) type "
                    + indexType + "(-180.0,-90.0,180.0,90.0);\n");
        }

        //create feed using socket_adapter
        sb.append("create feed TweetFeed using socket_adapter ((\"sockets\"=\"" + socketAdapterIp + ":" + socketAdapterPort + "\"),(\"addressType\"=\"IP\"),(\"type-name\"=\"FsqCheckinTweetType\"),(\"format\"=\"adm\"));\n");
        return sb.toString();
    }

    private static String getIngestionAQL() {
        StringBuilder sb = new StringBuilder();
        sb.append("use dataverse STBench;\n");
        sb.append("set wait-for-completion-feed \"false\";\n");
        sb.append("connect feed TweetFeed to dataset FsqCheckinTweet;\n");
        return sb.toString();
    }
    
    private static void runSocketCliet() {
        FeedSocketAdapterClient fsac = new FeedSocketAdapterClient(socketAdapterIp, socketAdapterPort, admFilePath, 10000000);
        fsac.initialize();
        fsac.ingest();
        fsac.finalize();
    }

}
