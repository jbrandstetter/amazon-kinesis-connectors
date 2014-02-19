/*
 * Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.connectors.mongodb;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.sun.corba.se.impl.logging.ORBUtilSystemException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;
import com.mongodb.*;

/**
 * This class is used to store records from a stream in a MongoDB table. It requires the use of a
 * MongoDBTransformer, which is able to transform records into a format that can be sent to
 * MongoDB. A MongoDB client is used to perform batch requests on the contents of a buffer when
 * emitting. This class requires the configuration of a DynamoDB endpoint and table name.
 */
public class MongoDBDBEmitter implements IEmitter<BasicDBObject> {
    private static final Log LOG = LogFactory.getLog(MongoDBDBEmitter.class);
    protected final String mongoDBEndpoint;
    protected final String mongoDBcollection;
    protected MongoClient mongoClient;
    protected final MongoClientURI uri;

    public MongoDBDBEmitter(KinesisConnectorConfiguration configuration) {
        // MongoDB Config from properties file
        this.mongoDBEndpoint = configuration.MONGODB_URI;
        this.mongoDBcollection = configuration.MONGODB_COLLECTION;

        // Client
        this.uri = new MongoClientURI(mongoDBEndpoint);
        try {
            this.mongoClient = new MongoClient(uri);
        } catch (UnknownHostException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        //this.mongoClient.setEndpoint(this.mongoDBEndpoint);
    }

    @Override
    public List<BasicDBObject> emit(final UnmodifiableBuffer<BasicDBObject> buffer)
            throws IOException {
        Set<BasicDBObject> uniqueItems = uniqueItems(buffer.getRecords());
        List <BasicDBObject> returnList = new ArrayList<BasicDBObject>();
        for (BasicDBObject id : uniqueItems){
            DB db = mongoClient.getDB(uri.getDatabase());
            DBCollection collection = db.getCollection(mongoDBcollection);
            collection.save(id);
            returnList.add(id);

            //System.out.println(id.toString());
            //System.out.println("emitting...");
            LOG.info("Successfully emitted " + (id.toString())
                    + " records into MongoDB.");
        }

        return returnList ;

    }

    @Override
    public void fail(List<BasicDBObject> records) {
        for (BasicDBObject record: records) {
            LOG.error("Could not emit record: " + record);
        }
    }

    /**
     * This method performs a batch request into DynamoDB and returns records that were
     * unsuccessfully processed by the batch request. Throws IOException if the client calls to
     * DynamoDB encounter an exception.
     *
     * @param rList
     *            list of WriteRequests to batch
     * @param requestMap
     *            map of WriteRequests to records
     * @return records that did not get put in the table by the batch request
     * @throws java.io.IOException
     *             if DynamoDB client encounters an exception
     */
    private List<BasicDBObject> performBatchRequest(List<BasicDBObject> rList,
            Map<String, BasicDBObject> requestMap) throws IOException {
        // Requests in the batch
        return rList;

    }

    /*private List<BasicDBObject> unproccessedItems( result,
            Map<WriteRequest, Map<String, AttributeValue>> requestMap) {

    }      */

    /**
     * This helper method is used to dedupe a list of items. Use this method to dedupe the contents
     * of a buffer before performing a DynamoDB batch write request.
     * 
     * @param items
     *            a list of Map<String,AttributeValue> items
     * @return the subset of unique items
     */
    public Set<BasicDBObject> uniqueItems(List<BasicDBObject> items) {
        return new HashSet<BasicDBObject>(items);
    }

    @Override
    public void shutdown() {
        mongoClient.close();
    }
}
