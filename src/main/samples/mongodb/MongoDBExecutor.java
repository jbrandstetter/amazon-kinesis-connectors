/*
 * Designed by comSysto GmbH, Germany
 */
package samples.mongodb;

import java.util.Map;

import com.mongodb.BasicDBObject;
import samples.KinesisConnectorExecutor;
import samples.KinesisMessageModel;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;

/**
 * The Executor for the MongoDB emitter sample.
 */
public class MongoDBExecutor extends KinesisConnectorExecutor<KinesisMessageModel, BasicDBObject> {

    private static String configFile = "MongoDBSample.properties";

    /**
     * Creates a new MongoDBExcecutor.
     * @param configFile The name of the configuration file to look for on the classpath.
     */
    public MongoDBExecutor(String configFile) {
        super(configFile);
    }

    @Override
    public KinesisConnectorRecordProcessorFactory<KinesisMessageModel, BasicDBObject> getKinesisConnectorRecordProcessorFactory() {
        return new KinesisConnectorRecordProcessorFactory<KinesisMessageModel, BasicDBObject>(
                new MongoDBMessageModelPipeline(), config);
    }

    /**
     * Main method starts and runs the MongoDBExecutor.
     * @param args
     */
    public static void main(String[] args) {
        KinesisConnectorExecutor<KinesisMessageModel, BasicDBObject> mongoDBExecutor = new MongoDBExecutor(configFile);
        mongoDBExecutor.run();
    }
}
