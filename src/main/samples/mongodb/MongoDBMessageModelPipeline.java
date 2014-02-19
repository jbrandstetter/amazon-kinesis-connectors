/*
 * Designed by comSysto GmbH, Germany
 */
package samples.mongodb;

import java.util.Map;

import com.mongodb.BasicDBObject;
import samples.KinesisMessageModel;


import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.mongodb.MongoDBDBEmitter;
import com.amazonaws.services.kinesis.connectors.impl.AllPassFilter;
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;

/**
 * The Pipeline used by the DynamoDB sample. Processes KinesisMessageModel records in JSON String
 * format. Uses: 
 * <ul>
 * <li>{@link com.amazonaws.services.kinesis.connectors.dynamodb.DynamoDBEmitter}</li>
 * <li>{@link com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer}</li>
 * <li>{@link KinesisMessageModelDynamoDBTransformer}</li>
 * <li>{@link com.amazonaws.services.kinesis.connectors.impl.AllPassFilter}</li>
 * </ul>
 */
public class MongoDBMessageModelPipeline implements IKinesisConnectorPipeline<KinesisMessageModel, BasicDBObject> {

    @Override
    public IEmitter<BasicDBObject> getEmitter(KinesisConnectorConfiguration configuration) {
        return new MongoDBDBEmitter(configuration);
    }

    @Override
    public IBuffer<KinesisMessageModel> getBuffer(KinesisConnectorConfiguration configuration) {
        return new BasicMemoryBuffer<KinesisMessageModel>(configuration);
    }

    @Override
    public ITransformer<KinesisMessageModel, BasicDBObject> getTransformer(KinesisConnectorConfiguration configuration) {
        return new KinesisMessageModelMongoDBTransformer();
    }

    @Override
    public IFilter<KinesisMessageModel> getFilter(KinesisConnectorConfiguration configuration) {
        return new AllPassFilter<KinesisMessageModel>();
    }

}
