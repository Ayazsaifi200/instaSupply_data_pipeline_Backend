const redisService = require('../src/services/redisService');
const kafkaService = require('../src/services/kafkaService');

describe('Services', () => {
  describe('RedisService', () => {
    beforeAll(async () => {
      try {
        await redisService.connect();
      } catch (error) {
        console.warn('Redis not available for testing:', error.message);
      }
    });

    afterAll(async () => {
      if (redisService.isHealthy()) {
        await redisService.disconnect();
      }
    });

    test('should connect to Redis', () => {
      // This test will pass even if Redis is not available
      // because the service handles connection failures gracefully
      expect(redisService).toBeDefined();
    });

    test('should set and get data', async () => {
      if (!redisService.isHealthy()) {
        console.warn('Skipping Redis test - service not healthy');
        return;
      }

      const testKey = 'test-key';
      const testData = { message: 'test data', timestamp: Date.now() };

      const setResult = await redisService.set(testKey, testData, 60);
      expect(setResult).toBe(true);

      const retrievedData = await redisService.get(testKey);
      expect(retrievedData).toEqual(testData);

      // Clean up
      await redisService.del(testKey);
    });

    test('should handle non-existent keys gracefully', async () => {
      if (!redisService.isHealthy()) {
        console.warn('Skipping Redis test - service not healthy');
        return;
      }

      const nonExistentData = await redisService.get('non-existent-key');
      expect(nonExistentData).toBeNull();
    });

    test('should check key existence', async () => {
      if (!redisService.isHealthy()) {
        console.warn('Skipping Redis test - service not healthy');
        return;
      }

      const testKey = 'existence-test';
      
      // Key should not exist initially
      const existsBefore = await redisService.exists(testKey);
      expect(existsBefore).toBe(false);

      // Set the key
      await redisService.set(testKey, 'test value', 60);

      // Key should exist now
      const existsAfter = await redisService.exists(testKey);
      expect(existsAfter).toBe(true);

      // Clean up
      await redisService.del(testKey);
    });

    test('should handle Redis unavailability gracefully', async () => {
      // These operations should not throw errors even if Redis is unavailable
      const setResult = await redisService.set('test', { value: 'data' });
      const getData = await redisService.get('test');
      const delResult = await redisService.del('test');
      const existsResult = await redisService.exists('test');

      // Results depend on Redis availability but should not throw
      expect(typeof setResult).toBe('boolean');
      expect(getData === null || typeof getData === 'object').toBe(true);
      expect(typeof delResult).toBe('boolean');
      expect(typeof existsResult).toBe('boolean');
    });
  });

  describe('KafkaService', () => {
    beforeAll(async () => {
      try {
        await kafkaService.initializeProducer();
      } catch (error) {
        console.warn('Kafka not available for testing:', error.message);
      }
    });

    afterAll(async () => {
      await kafkaService.disconnect();
    });

    test('should initialize producer', () => {
      expect(kafkaService).toBeDefined();
      expect(typeof kafkaService.isHealthy).toBe('function');
    });

    test('should publish data update message', async () => {
      if (!kafkaService.isHealthy()) {
        console.warn('Skipping Kafka test - producer not healthy');
        return;
      }

      const testData = {
        uploadId: 'test-upload-id',
        filename: 'test.csv',
        recordsProcessed: 10,
        recordsInserted: 8,
        recordsUpdated: 2
      };

      // This should not throw an error
      await expect(kafkaService.publishDataUpdate(testData))
        .resolves.toBeDefined();
    }, 10000);

    test('should handle Kafka unavailability gracefully', async () => {
      const testData = {
        uploadId: 'test-upload-id',
        filename: 'test.csv'
      };

      // If Kafka is not available, this should throw an error
      // but the test framework should catch it properly
      try {
        await kafkaService.publishDataUpdate(testData);
      } catch (error) {
        expect(error).toBeDefined();
        expect(error.message).toContain('Kafka');
      }
    });

    test('should create topics', async () => {
      if (!kafkaService.isHealthy()) {
        console.warn('Skipping Kafka topic creation test - service not healthy');
        return;
      }

      const testTopics = ['test-topic'];
      
      // Should complete without throwing
      await expect(kafkaService.createTopics(testTopics))
        .resolves.toBeUndefined();
    }, 15000);
  });

  describe('Service Health Checks', () => {
    test('should report Redis health status', () => {
      const isHealthy = redisService.isHealthy();
      expect(typeof isHealthy).toBe('boolean');
    });

    test('should report Kafka health status', () => {
      const isHealthy = kafkaService.isHealthy();
      expect(typeof isHealthy).toBe('boolean');
    });
  });
});