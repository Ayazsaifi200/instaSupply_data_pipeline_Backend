const { Kafka } = require('kafkajs');
const config = require('../config/config');
const logger = require('../utils/logger');

class KafkaService {
  constructor() {
    this.kafka = new Kafka({
      clientId: 'instasupply-app',
      brokers: config.kafka.brokers,
      retry: {
        initialRetryTime: 100,
        retries: 8
      }
    });
    this.producer = null;
    this.consumer = null;
    this.isProducerConnected = false;
    this.isConsumerConnected = false;
  }

  async initializeProducer() {
    try {
      this.producer = this.kafka.producer({
        maxInFlightRequests: 1,
        idempotent: true,
        transactionTimeout: 30000
      });

      await this.producer.connect();
      this.isProducerConnected = true;
      logger.info('Kafka producer connected successfully');
      return this.producer;
    } catch (error) {
      logger.error('Kafka producer connection failed:', error);
      this.isProducerConnected = false;
      throw error;
    }
  }

  async initializeConsumer() {
    try {
      this.consumer = this.kafka.consumer({
        groupId: config.kafka.groupId,
        sessionTimeout: 30000,
        heartbeatInterval: 3000
      });

      await this.consumer.connect();
      await this.consumer.subscribe({ topic: config.kafka.topic, fromBeginning: false });
      this.isConsumerConnected = true;
      logger.info('Kafka consumer connected and subscribed successfully');
      return this.consumer;
    } catch (error) {
      logger.error('Kafka consumer connection failed:', error);
      this.isConsumerConnected = false;
      throw error;
    }
  }

  async publishDataUpdate(data) {
    try {
      if (!this.isProducerConnected) {
        throw new Error('Kafka producer not connected');
      }

      const message = {
        key: data.uploadId || 'data-update',
        value: JSON.stringify({
          ...data,
          timestamp: new Date().toISOString(),
          eventType: 'data_uploaded'
        }),
        headers: {
          'content-type': 'application/json',
          'event-source': 'upload-api'
        }
      };

      const result = await this.producer.send({
        topic: config.kafka.topic,
        messages: [message]
      });

      logger.info('Data update published to Kafka:', {
        topic: config.kafka.topic,
        partition: result[0].partition,
        offset: result[0].baseOffset,
        uploadId: data.uploadId
      });

      return result;
    } catch (error) {
      logger.error('Failed to publish data update to Kafka:', error);
      throw error;
    }
  }

  async startConsumer(messageHandler) {
    try {
      if (!this.consumer) {
        await this.initializeConsumer();
      }

      await this.consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          try {
            const data = {
              topic,
              partition,
              offset: message.offset,
              key: message.key?.toString(),
              value: message.value?.toString(),
              headers: message.headers,
              timestamp: message.timestamp
            };

            logger.info('Processing Kafka message:', {
              topic,
              partition,
              offset: message.offset,
              key: data.key
            });

            await messageHandler(data);
            
            logger.debug('Message processed successfully:', {
              offset: message.offset,
              key: data.key
            });

          } catch (error) {
            logger.error('Error processing Kafka message:', {
              error: error.message,
              topic,
              partition,
              offset: message.offset
            });
            // Don't throw here to avoid stopping the consumer
            // In production, you might want to send to a dead letter queue
          }
        }
      });

      logger.info('Kafka consumer started successfully');
    } catch (error) {
      logger.error('Failed to start Kafka consumer:', error);
      throw error;
    }
  }

  async disconnect() {
    try {
      if (this.producer && this.isProducerConnected) {
        await this.producer.disconnect();
        this.isProducerConnected = false;
        logger.info('Kafka producer disconnected');
      }

      if (this.consumer && this.isConsumerConnected) {
        await this.consumer.disconnect();
        this.isConsumerConnected = false;
        logger.info('Kafka consumer disconnected');
      }
    } catch (error) {
      logger.error('Error disconnecting Kafka:', error);
    }
  }

  async createTopics(topics = [config.kafka.topic]) {
    try {
      const admin = this.kafka.admin();
      await admin.connect();

      const topicConfigs = topics.map(topic => ({
        topic,
        numPartitions: 3,
        replicationFactor: 1
      }));

      await admin.createTopics({
        topics: topicConfigs,
        waitForLeaders: true
      });

      logger.info('Kafka topics created:', topics);
      await admin.disconnect();
    } catch (error) {
      if (error.message.includes('already exists')) {
        logger.info('Kafka topics already exist:', topics);
      } else {
        logger.error('Failed to create Kafka topics:', error);
        throw error;
      }
    }
  }

  isHealthy() {
    return this.isProducerConnected || this.isConsumerConnected;
  }
}

// Create singleton instance
const kafkaService = new KafkaService();

module.exports = kafkaService;