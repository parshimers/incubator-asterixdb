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
package org.apache.asterix.external.util.aws.s3;

import static org.apache.asterix.common.exceptions.ErrorCode.INVALID_PARAM_VALUE_ALLOWED_VALUE;
import static org.apache.asterix.common.exceptions.ErrorCode.PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT;
import static org.apache.asterix.common.exceptions.ErrorCode.REQUIRED_PARAM_IF_PARAM_IS_PRESENT;
import static org.apache.asterix.common.exceptions.ErrorCode.S3_REGION_NOT_SUPPORTED;
import static org.apache.asterix.external.util.ExternalDataUtils.getPrefix;
import static org.apache.asterix.external.util.ExternalDataUtils.isDeltaTable;
import static org.apache.asterix.external.util.ExternalDataUtils.validateDeltaTableProperties;
import static org.apache.asterix.external.util.ExternalDataUtils.validateIncludeExclude;
import static org.apache.asterix.external.util.aws.s3.S3Constants.ACCESS_KEY_ID_FIELD_NAME;
import static org.apache.asterix.external.util.aws.s3.S3Constants.ERROR_INTERNAL_ERROR;
import static org.apache.asterix.external.util.aws.s3.S3Constants.ERROR_METHOD_NOT_IMPLEMENTED;
import static org.apache.asterix.external.util.aws.s3.S3Constants.ERROR_SLOW_DOWN;
import static org.apache.asterix.external.util.aws.s3.S3Constants.EXTERNAL_ID_FIELD_NAME;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_ACCESS_KEY_ID;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_ANONYMOUS_ACCESS;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_CREDENTIAL_PROVIDER_KEY;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_PATH_STYLE_ACCESS;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_S3_CONNECTION_POOL_SIZE;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_S3_PROTOCOL;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_SECRET_ACCESS_KEY;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_SERVICE_END_POINT;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_SESSION_TOKEN;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_TEMP_ACCESS;
import static org.apache.asterix.external.util.aws.s3.S3Constants.INSTANCE_PROFILE_FIELD_NAME;
import static org.apache.asterix.external.util.aws.s3.S3Constants.REGION_FIELD_NAME;
import static org.apache.asterix.external.util.aws.s3.S3Constants.ROLE_ARN_FIELD_NAME;
import static org.apache.asterix.external.util.aws.s3.S3Constants.SECRET_ACCESS_KEY_FIELD_NAME;
import static org.apache.asterix.external.util.aws.s3.S3Constants.SERVICE_END_POINT_FIELD_NAME;
import static org.apache.asterix.external.util.aws.s3.S3Constants.SESSION_TOKEN_FIELD_NAME;
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.external.IExternalFilterEvaluator;
import org.apache.asterix.external.input.record.reader.abstracts.AbstractExternalInputStreamFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataPrefix;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.HDFSUtils;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.api.util.CleanupUtils;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.S3Response;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

public class S3Utils {
    private S3Utils() {
        throw new AssertionError("do not instantiate");
    }

    public static boolean isRetryableError(String errorCode) {
        return errorCode.equals(ERROR_INTERNAL_ERROR) || errorCode.equals(ERROR_SLOW_DOWN);
    }

    /**
     * Builds the S3 client using the provided configuration
     *
     * @param configuration properties
     * @return S3 client
     * @throws CompilationException CompilationException
     */
    public static S3Client buildAwsS3Client(Map<String, String> configuration) throws CompilationException {
        String regionId = configuration.get(REGION_FIELD_NAME);
        String serviceEndpoint = configuration.get(SERVICE_END_POINT_FIELD_NAME);

        Region region = validateAndGetRegion(regionId);
        AwsCredentialsProvider credentialsProvider = buildCredentialsProvider(configuration);

        S3ClientBuilder builder = S3Client.builder();
        builder.region(region);
        builder.credentialsProvider(credentialsProvider);

        // Validate the service endpoint if present
        if (serviceEndpoint != null) {
            try {
                URI uri = new URI(serviceEndpoint);
                try {
                    builder.endpointOverride(uri);
                } catch (NullPointerException ex) {
                    throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
                }
            } catch (URISyntaxException ex) {
                throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex,
                        String.format("Invalid service endpoint %s", serviceEndpoint));
            }
        }

        return builder.build();
    }

    public static AwsCredentialsProvider buildCredentialsProvider(Map<String, String> configuration)
            throws CompilationException {
        String arnRole = configuration.get(ROLE_ARN_FIELD_NAME);
        String externalId = configuration.get(EXTERNAL_ID_FIELD_NAME);
        String instanceProfile = configuration.get(INSTANCE_PROFILE_FIELD_NAME);
        String accessKeyId = configuration.get(ACCESS_KEY_ID_FIELD_NAME);
        String secretAccessKey = configuration.get(SECRET_ACCESS_KEY_FIELD_NAME);

        if (noAuth(configuration)) {
            return AnonymousCredentialsProvider.create();
        } else if (arnRole != null) {
            // TODO: Do auth validation and use existing credentials if exist already, if not, assume the role
            return validateAndGetTrustAccountAuthentication(configuration);
        } else if (instanceProfile != null) {
            return validateAndGetInstanceProfileAuthentication(configuration);
        } else if (accessKeyId != null || secretAccessKey != null) {
            return validateAndGetAccessKeysAuthentications(configuration);
        } else {
            if (externalId != null) {
                throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, ROLE_ARN_FIELD_NAME,
                        EXTERNAL_ID_FIELD_NAME);
            } else {
                throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, ACCESS_KEY_ID_FIELD_NAME,
                        SESSION_TOKEN_FIELD_NAME);
            }
        }
    }

    /**
     * Builds the S3 client using the provided configuration
     *
     * @param configuration      properties
     * @param numberOfPartitions number of partitions in the cluster
     */
    public static void configureAwsS3HdfsJobConf(JobConf conf, Map<String, String> configuration,
            int numberOfPartitions) {
        String accessKeyId = configuration.get(ACCESS_KEY_ID_FIELD_NAME);
        String secretAccessKey = configuration.get(SECRET_ACCESS_KEY_FIELD_NAME);
        String sessionToken = configuration.get(SESSION_TOKEN_FIELD_NAME);
        String serviceEndpoint = configuration.get(SERVICE_END_POINT_FIELD_NAME);

        //Disable caching S3 FileSystem
        HDFSUtils.disableHadoopFileSystemCache(conf, HADOOP_S3_PROTOCOL);

        /*
         * Authentication Methods:
         * 1- Anonymous: no accessKeyId and no secretAccessKey
         * 2- Temporary: has to provide accessKeyId, secretAccessKey and sessionToken
         * 3- Private: has to provide accessKeyId and secretAccessKey
         */
        if (accessKeyId == null) {
            //Tells hadoop-aws it is an anonymous access
            conf.set(HADOOP_CREDENTIAL_PROVIDER_KEY, HADOOP_ANONYMOUS_ACCESS);
        } else {
            conf.set(HADOOP_ACCESS_KEY_ID, accessKeyId);
            conf.set(HADOOP_SECRET_ACCESS_KEY, secretAccessKey);
            if (sessionToken != null) {
                conf.set(HADOOP_SESSION_TOKEN, sessionToken);
                //Tells hadoop-aws it is a temporary access
                conf.set(HADOOP_CREDENTIAL_PROVIDER_KEY, HADOOP_TEMP_ACCESS);
            }
        }

        /*
         * This is to allow S3 definition to have path-style form. Should always be true to match the current
         * way we access files in S3
         */
        conf.set(HADOOP_PATH_STYLE_ACCESS, ExternalDataConstants.TRUE);

        /*
         * Set the size of S3 connection pool to be the number of partitions
         */
        conf.set(HADOOP_S3_CONNECTION_POOL_SIZE, String.valueOf(numberOfPartitions));

        if (serviceEndpoint != null) {
            // Validation of the URL should be done at hadoop-aws level
            conf.set(HADOOP_SERVICE_END_POINT, serviceEndpoint);
        } else {
            //Region is ignored and buckets could be found by the central endpoint
            conf.set(HADOOP_SERVICE_END_POINT, Constants.CENTRAL_ENDPOINT);
        }
    }

    /**
     * Validate external dataset properties
     *
     * @param configuration properties
     * @throws CompilationException Compilation exception
     */
    public static void validateProperties(Map<String, String> configuration, SourceLocation srcLoc,
            IWarningCollector collector) throws CompilationException {
        if (isDeltaTable(configuration)) {
            validateDeltaTableProperties(configuration);
        }
        // check if the format property is present
        else if (configuration.get(ExternalDataConstants.KEY_FORMAT) == null) {
            throw new CompilationException(ErrorCode.PARAMETERS_REQUIRED, srcLoc, ExternalDataConstants.KEY_FORMAT);
        }

        String arnRole = configuration.get(ROLE_ARN_FIELD_NAME);
        String externalId = configuration.get(EXTERNAL_ID_FIELD_NAME);
        String accessKeyId = configuration.get(ACCESS_KEY_ID_FIELD_NAME);
        String secretAccessKey = configuration.get(SECRET_ACCESS_KEY_FIELD_NAME);

        if (arnRole != null) {
            String notAllowed = getNonNull(configuration, ACCESS_KEY_ID_FIELD_NAME, SECRET_ACCESS_KEY_FIELD_NAME,
                    SESSION_TOKEN_FIELD_NAME);
            if (notAllowed != null) {
                throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, notAllowed,
                        INSTANCE_PROFILE_FIELD_NAME);
            }
        } else if (externalId != null) {
            throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, ROLE_ARN_FIELD_NAME,
                    EXTERNAL_ID_FIELD_NAME);
        } else if (accessKeyId == null || secretAccessKey == null) {
            // If one is passed, the other is required
            if (accessKeyId != null) {
                throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, SECRET_ACCESS_KEY_FIELD_NAME,
                        ACCESS_KEY_ID_FIELD_NAME);
            } else if (secretAccessKey != null) {
                throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, ACCESS_KEY_ID_FIELD_NAME,
                        SECRET_ACCESS_KEY_FIELD_NAME);
            }
        }

        validateIncludeExclude(configuration);
        try {
            // TODO(htowaileb): maybe something better, this will check to ensure type is supported before creation
            new ExternalDataPrefix(configuration);
        } catch (AlgebricksException ex) {
            throw new CompilationException(ErrorCode.FAILED_TO_CALCULATE_COMPUTED_FIELDS, ex);
        }

        // Check if the bucket is present
        S3Client s3Client = buildAwsS3Client(configuration);
        S3Response response;
        boolean useOldApi = false;
        String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
        String prefix = getPrefix(configuration);

        try {
            response = isBucketEmpty(s3Client, container, prefix, false);
        } catch (S3Exception ex) {
            // Method not implemented, try falling back to old API
            try {
                // For error code, see https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
                if (ex.awsErrorDetails().errorCode().equals(ERROR_METHOD_NOT_IMPLEMENTED)) {
                    useOldApi = true;
                    response = isBucketEmpty(s3Client, container, prefix, true);
                } else {
                    throw ex;
                }
            } catch (SdkException ex2) {
                throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex2, getMessageOrToString(ex));
            }
        } catch (SdkException ex) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
        } finally {
            if (s3Client != null) {
                CleanupUtils.close(s3Client, null);
            }
        }

        boolean isEmpty = useOldApi ? ((ListObjectsResponse) response).contents().isEmpty()
                : ((ListObjectsV2Response) response).contents().isEmpty();
        if (isEmpty && collector.shouldWarn()) {
            Warning warning = Warning.of(srcLoc, ErrorCode.EXTERNAL_SOURCE_CONFIGURATION_RETURNED_NO_FILES);
            collector.warn(warning);
        }

        // Returns 200 only in case the bucket exists, otherwise, throws an exception. However, to
        // ensure coverage, check if the result is successful as well and not only catch exceptions
        if (!response.sdkHttpResponse().isSuccessful()) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_CONTAINER_NOT_FOUND, container);
        }
    }

    /**
     * Checks for a single object in the specified bucket to determine if the bucket is empty or not.
     *
     * @param s3Client  s3 client
     * @param container the container name
     * @param prefix    Prefix to be used
     * @param useOldApi flag whether to use the old API or not
     * @return returns the S3 response
     */
    private static S3Response isBucketEmpty(S3Client s3Client, String container, String prefix, boolean useOldApi) {
        S3Response response;
        if (useOldApi) {
            ListObjectsRequest.Builder listObjectsBuilder = ListObjectsRequest.builder();
            listObjectsBuilder.prefix(prefix);
            response = s3Client.listObjects(listObjectsBuilder.bucket(container).maxKeys(1).build());
        } else {
            ListObjectsV2Request.Builder listObjectsBuilder = ListObjectsV2Request.builder();
            listObjectsBuilder.prefix(prefix);
            response = s3Client.listObjectsV2(listObjectsBuilder.bucket(container).maxKeys(1).build());
        }
        return response;
    }

    /**
     * Returns the lists of S3 objects.
     *
     * @param configuration         properties
     * @param includeExcludeMatcher include/exclude matchers to apply
     */
    public static List<S3Object> listS3Objects(Map<String, String> configuration,
            AbstractExternalInputStreamFactory.IncludeExcludeMatcher includeExcludeMatcher,
            IWarningCollector warningCollector, ExternalDataPrefix externalDataPrefix,
            IExternalFilterEvaluator evaluator) throws CompilationException, HyracksDataException {
        // Prepare to retrieve the objects
        List<S3Object> filesOnly;
        String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
        S3Client s3Client = buildAwsS3Client(configuration);
        String prefix = getPrefix(configuration);

        try {
            filesOnly = listS3Objects(s3Client, container, prefix, includeExcludeMatcher, externalDataPrefix, evaluator,
                    warningCollector);
        } catch (S3Exception ex) {
            // New API is not implemented, try falling back to old API
            try {
                // For error code, see https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
                if (ex.awsErrorDetails().errorCode().equals(ERROR_METHOD_NOT_IMPLEMENTED)) {
                    filesOnly = oldApiListS3Objects(s3Client, container, prefix, includeExcludeMatcher,
                            externalDataPrefix, evaluator, warningCollector);
                } else {
                    throw ex;
                }
            } catch (SdkException ex2) {
                throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex2, getMessageOrToString(ex));
            }
        } catch (SdkException ex) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
        } finally {
            if (s3Client != null) {
                CleanupUtils.close(s3Client, null);
            }
        }

        // Warn if no files are returned
        if (filesOnly.isEmpty() && warningCollector.shouldWarn()) {
            Warning warning = Warning.of(null, ErrorCode.EXTERNAL_SOURCE_CONFIGURATION_RETURNED_NO_FILES);
            warningCollector.warn(warning);
        }

        return filesOnly;
    }

    /**
     * Uses the latest API to retrieve the objects from the storage.
     *
     * @param s3Client              S3 client
     * @param container             container name
     * @param prefix                definition prefix
     * @param includeExcludeMatcher include/exclude matchers to apply
     */
    private static List<S3Object> listS3Objects(S3Client s3Client, String container, String prefix,
            AbstractExternalInputStreamFactory.IncludeExcludeMatcher includeExcludeMatcher,
            ExternalDataPrefix externalDataPrefix, IExternalFilterEvaluator evaluator,
            IWarningCollector warningCollector) throws HyracksDataException {
        String newMarker = null;
        List<S3Object> filesOnly = new ArrayList<>();

        ListObjectsV2Response listObjectsResponse;
        ListObjectsV2Request.Builder listObjectsBuilder = ListObjectsV2Request.builder().bucket(container);
        listObjectsBuilder.prefix(prefix);

        while (true) {
            // List the objects from the start, or from the last marker in case of truncated result
            if (newMarker == null) {
                listObjectsResponse = s3Client.listObjectsV2(listObjectsBuilder.build());
            } else {
                listObjectsResponse = s3Client.listObjectsV2(listObjectsBuilder.continuationToken(newMarker).build());
            }

            // Collect the paths to files only
            collectAndFilterFiles(listObjectsResponse.contents(), includeExcludeMatcher.getPredicate(),
                    includeExcludeMatcher.getMatchersList(), filesOnly, externalDataPrefix, evaluator,
                    warningCollector);

            // Mark the flag as done if done, otherwise, get the marker of the previous response for the next request
            if (listObjectsResponse.isTruncated() != null && listObjectsResponse.isTruncated()) {
                newMarker = listObjectsResponse.nextContinuationToken();
            } else {
                break;
            }
        }

        return filesOnly;
    }

    /**
     * Uses the old API (in case the new API is not implemented) to retrieve the objects from the storage
     *
     * @param s3Client              S3 client
     * @param container             container name
     * @param prefix                definition prefix
     * @param includeExcludeMatcher include/exclude matchers to apply
     */
    private static List<S3Object> oldApiListS3Objects(S3Client s3Client, String container, String prefix,
            AbstractExternalInputStreamFactory.IncludeExcludeMatcher includeExcludeMatcher,
            ExternalDataPrefix externalDataPrefix, IExternalFilterEvaluator evaluator,
            IWarningCollector warningCollector) throws HyracksDataException {
        String newMarker = null;
        List<S3Object> filesOnly = new ArrayList<>();

        ListObjectsResponse listObjectsResponse;
        ListObjectsRequest.Builder listObjectsBuilder = ListObjectsRequest.builder().bucket(container);
        listObjectsBuilder.prefix(prefix);

        while (true) {
            // List the objects from the start, or from the last marker in case of truncated result
            if (newMarker == null) {
                listObjectsResponse = s3Client.listObjects(listObjectsBuilder.build());
            } else {
                listObjectsResponse = s3Client.listObjects(listObjectsBuilder.marker(newMarker).build());
            }

            // Collect the paths to files only
            collectAndFilterFiles(listObjectsResponse.contents(), includeExcludeMatcher.getPredicate(),
                    includeExcludeMatcher.getMatchersList(), filesOnly, externalDataPrefix, evaluator,
                    warningCollector);

            // Mark the flag as done if done, otherwise, get the marker of the previous response for the next request
            if (listObjectsResponse.isTruncated() != null && listObjectsResponse.isTruncated()) {
                newMarker = listObjectsResponse.nextMarker();
            } else {
                break;
            }
        }

        return filesOnly;
    }

    /**
     * Collects only files that pass all tests
     *
     * @param s3Objects          s3 objects
     * @param predicate          predicate
     * @param matchers           matchers
     * @param filesOnly          filtered files
     * @param externalDataPrefix external data prefix
     * @param evaluator          evaluator
     */
    private static void collectAndFilterFiles(List<S3Object> s3Objects, BiPredicate<List<Matcher>, String> predicate,
            List<Matcher> matchers, List<S3Object> filesOnly, ExternalDataPrefix externalDataPrefix,
            IExternalFilterEvaluator evaluator, IWarningCollector warningCollector) throws HyracksDataException {
        for (S3Object object : s3Objects) {
            if (ExternalDataUtils.evaluate(object.key(), predicate, matchers, externalDataPrefix, evaluator,
                    warningCollector)) {
                filesOnly.add(object);
            }
        }
    }

    public static Map<String, List<String>> S3ObjectsOfSingleDepth(Map<String, String> configuration, String container,
            String prefix) throws CompilationException {
        // create s3 client
        S3Client s3Client = buildAwsS3Client(configuration);
        // fetch all the s3 objects
        return listS3ObjectsOfSingleDepth(s3Client, container, prefix);
    }

    /**
     * Uses the latest API to retrieve the objects from the storage of a single level.
     *
     * @param s3Client              S3 client
     * @param container             container name
     * @param prefix                definition prefix
     */
    private static Map<String, List<String>> listS3ObjectsOfSingleDepth(S3Client s3Client, String container,
            String prefix) {
        Map<String, List<String>> allObjects = new HashMap<>();
        ListObjectsV2Iterable listObjectsInterable;
        ListObjectsV2Request.Builder listObjectsBuilder =
                ListObjectsV2Request.builder().bucket(container).prefix(prefix).delimiter("/");
        listObjectsBuilder.prefix(prefix);
        List<String> files = new ArrayList<>();
        List<String> folders = new ArrayList<>();
        // to skip the prefix as a file from the response
        boolean checkPrefixInFile = true;
        listObjectsInterable = s3Client.listObjectsV2Paginator(listObjectsBuilder.build());
        for (ListObjectsV2Response response : listObjectsInterable) {
            // put all the files
            for (S3Object object : response.contents()) {
                String fileName = object.key();
                fileName = fileName.substring(prefix.length(), fileName.length());
                if (checkPrefixInFile) {
                    if (prefix.equals(object.key()))
                        checkPrefixInFile = false;
                    else {
                        files.add(fileName);
                    }
                } else {
                    files.add(fileName);
                }
            }
            // put all the folders
            for (CommonPrefix object : response.commonPrefixes()) {
                String folderName = object.prefix();
                folderName = folderName.substring(prefix.length(), folderName.length());
                folders.add(folderName.endsWith("/") ? folderName.substring(0, folderName.length() - 1) : folderName);
            }
        }
        allObjects.put("files", files);
        allObjects.put("folders", folders);
        return allObjects;
    }

    public static Region validateAndGetRegion(String regionId) throws CompilationException {
        List<Region> regions = S3Client.serviceMetadata().regions();
        Optional<Region> selectedRegion = regions.stream().filter(region -> region.id().equals(regionId)).findFirst();

        if (selectedRegion.isEmpty()) {
            throw new CompilationException(S3_REGION_NOT_SUPPORTED, regionId);
        }
        return selectedRegion.get();
    }

    // TODO(htowaileb): Currently, trust-account is always assuming we have instance profile setup in place
    private static AwsCredentialsProvider validateAndGetTrustAccountAuthentication(Map<String, String> configuration)
            throws CompilationException {
        String notAllowed = getNonNull(configuration, ACCESS_KEY_ID_FIELD_NAME, SECRET_ACCESS_KEY_FIELD_NAME,
                SESSION_TOKEN_FIELD_NAME);
        if (notAllowed != null) {
            throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, notAllowed,
                    INSTANCE_PROFILE_FIELD_NAME);
        }

        String regionId = configuration.get(REGION_FIELD_NAME);
        String arnRole = configuration.get(ROLE_ARN_FIELD_NAME);
        String externalId = configuration.get(EXTERNAL_ID_FIELD_NAME);
        Region region = validateAndGetRegion(regionId);

        AssumeRoleRequest.Builder builder = AssumeRoleRequest.builder();
        builder.roleArn(arnRole);
        builder.roleSessionName(UUID.randomUUID().toString());
        builder.durationSeconds(900); // minimum role assume duration = 900 seconds (15 minutes), make configurable?
        if (externalId != null) {
            builder.externalId(externalId);
        }
        AssumeRoleRequest request = builder.build();
        AwsCredentialsProvider credentialsProvider = validateAndGetInstanceProfileAuthentication(configuration);

        // TODO(htowaileb): We shouldn't assume role with each request, rather stored the received temporary credentials
        // and refresh when expired.
        // assume the role from the provided arn
        try (StsClient stsClient =
                StsClient.builder().region(region).credentialsProvider(credentialsProvider).build()) {
            AssumeRoleResponse response = stsClient.assumeRole(request);
            Credentials credentials = response.credentials();
            return StaticCredentialsProvider.create(AwsSessionCredentials.create(credentials.accessKeyId(),
                    credentials.secretAccessKey(), credentials.sessionToken()));
        } catch (SdkException ex) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
        }
    }

    private static AwsCredentialsProvider validateAndGetInstanceProfileAuthentication(Map<String, String> configuration)
            throws CompilationException {
        String instanceProfile = configuration.get(INSTANCE_PROFILE_FIELD_NAME);

        // only "true" value is allowed
        if (!"true".equalsIgnoreCase(instanceProfile)) {
            throw new CompilationException(INVALID_PARAM_VALUE_ALLOWED_VALUE, INSTANCE_PROFILE_FIELD_NAME, "true");
        }

        String notAllowed = getNonNull(configuration, ACCESS_KEY_ID_FIELD_NAME, SECRET_ACCESS_KEY_FIELD_NAME,
                SESSION_TOKEN_FIELD_NAME);
        if (notAllowed != null) {
            throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, notAllowed,
                    INSTANCE_PROFILE_FIELD_NAME);
        }
        return InstanceProfileCredentialsProvider.create();
    }

    private static AwsCredentialsProvider validateAndGetAccessKeysAuthentications(Map<String, String> configuration)
            throws CompilationException {
        String accessKeyId = configuration.get(ACCESS_KEY_ID_FIELD_NAME);
        String secretAccessKey = configuration.get(SECRET_ACCESS_KEY_FIELD_NAME);
        String sessionToken = configuration.get(SESSION_TOKEN_FIELD_NAME);

        // accessKeyId authentication
        if (accessKeyId == null) {
            throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, ACCESS_KEY_ID_FIELD_NAME,
                    SECRET_ACCESS_KEY_FIELD_NAME);
        }
        if (secretAccessKey == null) {
            throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, SECRET_ACCESS_KEY_FIELD_NAME,
                    ACCESS_KEY_ID_FIELD_NAME);
        }

        String notAllowed = getNonNull(configuration, EXTERNAL_ID_FIELD_NAME);
        if (notAllowed != null) {
            throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, notAllowed,
                    INSTANCE_PROFILE_FIELD_NAME);
        }

        // use session token if provided
        if (sessionToken != null) {
            return StaticCredentialsProvider
                    .create(AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken));
        } else {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey));
        }
    }

    private static boolean noAuth(Map<String, String> configuration) {
        return getNonNull(configuration, INSTANCE_PROFILE_FIELD_NAME, ROLE_ARN_FIELD_NAME, EXTERNAL_ID_FIELD_NAME,
                ACCESS_KEY_ID_FIELD_NAME, SECRET_ACCESS_KEY_FIELD_NAME, SESSION_TOKEN_FIELD_NAME) == null;
    }

    private static String getNonNull(Map<String, String> configuration, String... fieldNames) {
        for (String fieldName : fieldNames) {
            if (configuration.get(fieldName) != null) {
                return fieldName;
            }
        }
        return null;
    }
}
