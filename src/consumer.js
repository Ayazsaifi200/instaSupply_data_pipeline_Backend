const config = require('./config/config');
const logger = require('./utils/logger');
const database = require('./database/connection');
const redisService = require('./services/redisService');
const kafkaService = require('./services/kafkaService');

class KafkaConsumerService {
  constructor() {
    this.isRunning = false;
    this.isShuttingDown = false;
    this.processedMessages = 0;
    this.errorCount = 0;
    this.startTime = null;
  }

  async initialize() {
    try {
      logger.info('Initializing Kafka Consumer Service...');

      // Connect to database
      await database.connect();
      logger.info('Database connected');

      // Connect to Redis
      try {
        await redisService.connect();
        logger.info('Redis connected');
      } catch (error) {
        logger.warn('Redis connection failed, cache updates will be skipped:', error.message);
      }

      // Initialize Kafka consumer
      await kafkaService.initializeConsumer();
      logger.info('Kafka consumer initialized');

      // Setup graceful shutdown
      this.setupGracefulShutdown();

      logger.info('Kafka Consumer Service initialized successfully');
    } catch (error) {
      logger.error('Failed to initialize Kafka Consumer Service:', error);
      throw error;
    }
  }

  async start() {
    try {
      this.isRunning = true;
      this.startTime = new Date();
      
      logger.info('Starting Kafka consumer...');

      // Start consuming messages
      await kafkaService.startConsumer(this.handleMessage.bind(this));

      logger.info('Kafka Consumer Service started successfully', {
        topic: config.kafka.topic,
        groupId: config.kafka.groupId,
        startTime: this.startTime
      });

    } catch (error) {
      logger.error('Failed to start Kafka Consumer Service:', error);
      this.isRunning = false;
      throw error;
    }
  }

  async handleMessage(messageData) {
    const { topic, partition, offset, key, value, timestamp } = messageData;
    
    try {
      logger.info('Processing Kafka message:', {
        topic,
        partition,
        offset,
        key,
        timestamp: new Date(parseInt(timestamp)).toISOString()
      });

      // Parse message data
      const eventData = JSON.parse(value);
      
      // Validate message structure
      if (!this.isValidEventData(eventData)) {
        throw new Error('Invalid message structure');
      }

      // Process the data update event
      await this.processDataUpdateEvent(eventData);

      // Update cache if Redis is available
      await this.updateCache(eventData);

      // Log successful processing
      this.processedMessages++;
      logger.info('Message processed successfully:', {
        uploadId: eventData.uploadId,
        recordsInserted: eventData.recordsInserted,
        recordsUpdated: eventData.recordsUpdated,
        totalProcessed: this.processedMessages
      });

    } catch (error) {
      this.errorCount++;
      logger.error('Failed to process Kafka message:', {
        error: error.message,
        topic,
        partition,
        offset,
        key,
        errorCount: this.errorCount,
        messageValue: value
      });
      
      // In a production environment, you might want to:
      // 1. Send to dead letter queue
      // 2. Implement retry logic with exponential backoff
      // 3. Alert monitoring systems
    }
  }

  isValidEventData(eventData) {
    return eventData &&
           typeof eventData.uploadId === 'string' &&
           typeof eventData.filename === 'string' &&
           typeof eventData.recordsProcessed === 'number' &&
           eventData.eventType === 'data_uploaded';
  }

  async processDataUpdateEvent(eventData) {
    try {
      // Log the data update event to database
      const query = `
        INSERT INTO upload_events (
          id, upload_id, event_type, filename, records_processed,
          records_inserted, records_updated, processing_time_ms, 
          event_timestamp, processed_by_consumer
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      `;

      const params = [
        require('uuid').v4(),
        eventData.uploadId,
        eventData.eventType || 'data_uploaded',
        eventData.filename,
        eventData.recordsProcessed || 0,
        eventData.recordsInserted || 0,
        eventData.recordsUpdated || 0,
        eventData.processingTime || 0,
        new Date(eventData.timestamp),
        new Date()
      ];

      await database.executeQuery(query, params);
      logger.debug('Upload event logged to database');

      // Update upload log status
      const updateQuery = `
        UPDATE upload_logs 
        SET status = 'completed'
        WHERE id = $1
      `;
      
      await database.executeQuery(updateQuery, [eventData.uploadId]);

    } catch (error) {
      // Create table if it doesn't exist (for first run)
      if (error.message.includes('upload_events') && error.message.includes('does not exist')) {
        await this.createUploadEventsTable();
        // Retry the operation
        return this.processDataUpdateEvent(eventData);
      }
      throw error;
    }
  }

  async createUploadEventsTable() {
    try {
      const createTableQuery = `
        CREATE TABLE IF NOT EXISTS upload_events (
          id UUID PRIMARY KEY,
          upload_id UUID NOT NULL,
          event_type VARCHAR(50) NOT NULL,
          filename VARCHAR(255),
          records_processed INTEGER DEFAULT 0,
          records_inserted INTEGER DEFAULT 0,
          records_updated INTEGER DEFAULT 0,
          processing_time_ms INTEGER DEFAULT 0,
          event_timestamp TIMESTAMP,
          processed_by_consumer TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          FOREIGN KEY (upload_id) REFERENCES upload_logs(id)
        );
        
        CREATE INDEX IF NOT EXISTS idx_upload_events_upload_id ON upload_events(upload_id);
        CREATE INDEX IF NOT EXISTS idx_upload_events_timestamp ON upload_events(event_timestamp);
      `;

      await database.executeQuery(createTableQuery);
      logger.info('upload_events table created');
    } catch (error) {
      logger.error('Failed to create upload_events table:', error);
      throw error;
    }
  }

  async updateCache(eventData) {
    try {
      if (!redisService.isHealthy()) {
        logger.debug('Redis not available, skipping cache update');
        return;
      }

      // Clear the cached records to force fresh fetch on next request
      await redisService.del('all_records');
      
      // Clear stats cache as well since data has changed
      await redisService.del('record_stats');

      logger.debug('Cache cleared after data update', {
        uploadId: eventData.uploadId
      });

    } catch (error) {
      logger.warn('Failed to update cache (non-critical):', error.message);
    }
  }

  async getServiceStatus() {
    return {
      serviceName: 'Kafka Consumer Service',
      isRunning: this.isRunning,
      startTime: this.startTime,
      uptime: this.startTime ? Date.now() - this.startTime.getTime() : 0,
      processedMessages: this.processedMessages,
      errorCount: this.errorCount,
      successRate: this.processedMessages > 0 ? 
        ((this.processedMessages / (this.processedMessages + this.errorCount)) * 100).toFixed(2) + '%' : '0%',
      services: {
        database: database.getPool() ? 'connected' : 'disconnected',
        redis: redisService.isHealthy() ? 'connected' : 'disconnected',
        kafka: kafkaService.isHealthy() ? 'connected' : 'disconnected'
      },
      config: {
        topic: config.kafka.topic,
        groupId: config.kafka.groupId,
        brokers: config.kafka.brokers
      }
    };
  }

  setupGracefulShutdown() {
    const shutdown = async (signal) => {
      if (this.isShuttingDown) {
        logger.warn(`Received ${signal} but already shutting down`);
        return;
      }

      this.isShuttingDown = true;
      this.isRunning = false;
      
      logger.info(`Received ${signal}, starting graceful shutdown`);

      try {
        // Disconnect services
        await kafkaService.disconnect();
        await redisService.disconnect();
        await database.disconnect();

        const status = await this.getServiceStatus();
        logger.info('Kafka Consumer Service shutdown complete', status);
        
        process.exit(0);
      } catch (error) {
        logger.error('Error during shutdown:', error);
        process.exit(1);
      }
    };

    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));
    
    process.on('uncaughtException', (error) => {
      logger.error('Uncaught exception in consumer:', error);
      shutdown('uncaughtException');
    });
    
    process.on('unhandledRejection', (reason, promise) => {
      logger.error('Unhandled rejection in consumer:', { reason, promise });
      shutdown('unhandledRejection');
    });
  }
}

// Start the consumer service
async function startConsumerService() {
  try {
    const consumerService = new KafkaConsumerService();
    await consumerService.initialize();
    await consumerService.start();

    // Log status periodically
    setInterval(async () => {
      const status = await consumerService.getServiceStatus();
      logger.info('Consumer service status:', status);
    }, 60000); // Every minute

  } catch (error) {
    logger.error('Failed to start Kafka Consumer Service:', error);
    process.exit(1);
  }
}

// Start the service if this file is run directly
if (require.main === module) {
  startConsumerService();
}

module.exports = { KafkaConsumerService, startConsumerService };