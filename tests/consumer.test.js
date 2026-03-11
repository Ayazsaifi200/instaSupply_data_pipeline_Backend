const { KafkaConsumerService } = require('../src/consumer');
const database = require('../src/database/connection');
const redisService = require('../src/services/redisService');
const kafkaService = require('../src/services/kafkaService');

// Mock all external service dependencies so tests run without live infrastructure
jest.mock('../src/database/connection', () => ({
  connect: jest.fn().mockResolvedValue(true),
  disconnect: jest.fn().mockResolvedValue(true),
  executeQuery: jest.fn().mockResolvedValue({ rows: [] }),
  getPool: jest.fn().mockReturnValue({}),
}));

jest.mock('../src/services/redisService', () => ({
  connect: jest.fn().mockResolvedValue(true),
  disconnect: jest.fn().mockResolvedValue(true),
  del: jest.fn().mockResolvedValue(true),
  isHealthy: jest.fn().mockReturnValue(true),
}));

jest.mock('../src/services/kafkaService', () => ({
  initializeConsumer: jest.fn().mockResolvedValue(true),
  startConsumer: jest.fn().mockResolvedValue(true),
  disconnect: jest.fn().mockResolvedValue(true),
  isHealthy: jest.fn().mockReturnValue(true),
}));

// ─── Helpers ────────────────────────────────────────────────────────────────

function buildValidMessage(overrides = {}) {
  return {
    topic: 'data-updates',
    partition: 0,
    offset: '1',
    key: 'upload-key',
    timestamp: Date.now().toString(),
    value: JSON.stringify({
      uploadId: 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
      filename: 'test.csv',
      recordsProcessed: 10,
      recordsInserted: 8,
      recordsUpdated: 2,
      processingTime: 120,
      timestamp: new Date().toISOString(),
      eventType: 'data_uploaded',
      ...overrides
    })
  };
}

// ─── Tests ───────────────────────────────────────────────────────────────────

describe('KafkaConsumerService', () => {

  let consumer;

  beforeEach(() => {
    consumer = new KafkaConsumerService();
    jest.clearAllMocks();
    // Default: Redis is healthy
    redisService.isHealthy.mockReturnValue(true);
  });

  // ── Constructor ─────────────────────────────────────────────────────────

  describe('Constructor', () => {
    test('should initialise with correct default state', () => {
      expect(consumer.isRunning).toBe(false);
      expect(consumer.isShuttingDown).toBe(false);
      expect(consumer.processedMessages).toBe(0);
      expect(consumer.errorCount).toBe(0);
      expect(consumer.startTime).toBeNull();
    });
  });

  // ── initialize() ────────────────────────────────────────────────────────

  describe('initialize()', () => {
    test('should connect database, Redis and Kafka consumer', async () => {
      await consumer.initialize();

      expect(database.connect).toHaveBeenCalledTimes(1);
      expect(redisService.connect).toHaveBeenCalledTimes(1);
      expect(kafkaService.initializeConsumer).toHaveBeenCalledTimes(1);
    });

    test('should continue when Redis connection fails', async () => {
      redisService.connect.mockRejectedValueOnce(new Error('Redis unavailable'));

      await expect(consumer.initialize()).resolves.not.toThrow();
      expect(database.connect).toHaveBeenCalledTimes(1);
      expect(kafkaService.initializeConsumer).toHaveBeenCalledTimes(1);
    });

    test('should throw when database connection fails', async () => {
      database.connect.mockRejectedValueOnce(new Error('DB connection refused'));

      await expect(consumer.initialize()).rejects.toThrow('DB connection refused');
    });

    test('should throw when Kafka consumer init fails', async () => {
      kafkaService.initializeConsumer.mockRejectedValueOnce(new Error('Kafka unreachable'));

      await expect(consumer.initialize()).rejects.toThrow('Kafka unreachable');
    });
  });

  // ── start() ─────────────────────────────────────────────────────────────

  describe('start()', () => {
    test('should set isRunning and startTime, then call startConsumer', async () => {
      await consumer.start();

      expect(consumer.isRunning).toBe(true);
      expect(consumer.startTime).toBeInstanceOf(Date);
      expect(kafkaService.startConsumer).toHaveBeenCalledWith(
        expect.any(Function)
      );
    });

    test('should pass handleMessage as the callback to startConsumer', async () => {
      await consumer.start();

      const passedCallback = kafkaService.startConsumer.mock.calls[0][0];
      expect(passedCallback).toBeInstanceOf(Function);
    });

    test('should set isRunning to false when startConsumer throws', async () => {
      kafkaService.startConsumer.mockRejectedValueOnce(new Error('Start failed'));

      await expect(consumer.start()).rejects.toThrow('Start failed');
      expect(consumer.isRunning).toBe(false);
    });
  });

  // ── isValidEventData() ───────────────────────────────────────────────────

  describe('isValidEventData()', () => {
    test('should return true for a fully valid event object', () => {
      const data = {
        uploadId: 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
        filename: 'test.csv',
        recordsProcessed: 10,
        eventType: 'data_uploaded'
      };
      expect(consumer.isValidEventData(data)).toBe(true);
    });

    test('should return false when uploadId is missing', () => {
      const data = { filename: 'test.csv', recordsProcessed: 5, eventType: 'data_uploaded' };
      expect(consumer.isValidEventData(data)).toBe(false);
    });

    test('should return false when filename is missing', () => {
      const data = { uploadId: 'abc', recordsProcessed: 5, eventType: 'data_uploaded' };
      expect(consumer.isValidEventData(data)).toBe(false);
    });

    test('should return false when recordsProcessed is not a number', () => {
      const data = { uploadId: 'abc', filename: 'test.csv', recordsProcessed: '5', eventType: 'data_uploaded' };
      expect(consumer.isValidEventData(data)).toBe(false);
    });

    test('should return false when eventType is wrong', () => {
      const data = { uploadId: 'abc', filename: 'test.csv', recordsProcessed: 5, eventType: 'wrong_event' };
      expect(consumer.isValidEventData(data)).toBe(false);
    });

    test('should return false for null input', () => {
      expect(consumer.isValidEventData(null)).toBeFalsy();
    });

    test('should return false for empty object', () => {
      expect(consumer.isValidEventData({})).toBe(false);
    });
  });

  // ── handleMessage() ──────────────────────────────────────────────────────

  describe('handleMessage()', () => {
    test('should process a valid message and increment processedMessages', async () => {
      const message = buildValidMessage();

      await consumer.handleMessage(message);

      expect(consumer.processedMessages).toBe(1);
      expect(consumer.errorCount).toBe(0);
      expect(database.executeQuery).toHaveBeenCalled();
    });

    test('should process multiple valid messages sequentially', async () => {
      await consumer.handleMessage(buildValidMessage());
      await consumer.handleMessage(buildValidMessage());
      await consumer.handleMessage(buildValidMessage());

      expect(consumer.processedMessages).toBe(3);
      expect(consumer.errorCount).toBe(0);
    });

    test('should increment errorCount for invalid JSON in value', async () => {
      const message = { ...buildValidMessage(), value: 'not-valid-json{{' };

      await consumer.handleMessage(message);

      expect(consumer.errorCount).toBe(1);
      expect(consumer.processedMessages).toBe(0);
    });

    test('should increment errorCount for invalid event structure', async () => {
      const message = {
        ...buildValidMessage(),
        value: JSON.stringify({ badField: true })
      };

      await consumer.handleMessage(message);

      expect(consumer.errorCount).toBe(1);
      expect(consumer.processedMessages).toBe(0);
    });

    test('should increment errorCount when database.executeQuery throws', async () => {
      database.executeQuery.mockRejectedValueOnce(new Error('DB write failed'));

      await consumer.handleMessage(buildValidMessage());

      expect(consumer.errorCount).toBe(1);
      expect(consumer.processedMessages).toBe(0);
    });

    test('should handle missing optional fields gracefully (recordsInserted/Updated)', async () => {
      const message = buildValidMessage({ recordsInserted: undefined, recordsUpdated: undefined });

      await consumer.handleMessage(message);

      expect(consumer.processedMessages).toBe(1);
    });

    test('should not throw when message value is null', async () => {
      const message = { ...buildValidMessage(), value: null };

      await expect(consumer.handleMessage(message)).resolves.not.toThrow();
      expect(consumer.errorCount).toBe(1);
    });
  });

  // ── processDataUpdateEvent() ─────────────────────────────────────────────

  describe('processDataUpdateEvent()', () => {
    const validEvent = {
      uploadId: 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
      filename: 'test.csv',
      recordsProcessed: 10,
      recordsInserted: 8,
      recordsUpdated: 2,
      processingTime: 120,
      timestamp: new Date().toISOString(),
      eventType: 'data_uploaded'
    };

    test('should execute two DB queries: INSERT into upload_events and UPDATE upload_logs', async () => {
      await consumer.processDataUpdateEvent(validEvent);

      expect(database.executeQuery).toHaveBeenCalledTimes(2);

      const firstCall = database.executeQuery.mock.calls[0][0];
      const secondCall = database.executeQuery.mock.calls[1][0];

      expect(firstCall).toMatch(/INSERT INTO upload_events/i);
      expect(secondCall).toMatch(/UPDATE upload_logs/i);
    });

    test('should pass correct params to INSERT query', async () => {
      await consumer.processDataUpdateEvent(validEvent);

      const insertParams = database.executeQuery.mock.calls[0][1];
      expect(insertParams[1]).toBe(validEvent.uploadId);
      expect(insertParams[3]).toBe(validEvent.filename);
      expect(insertParams[4]).toBe(validEvent.recordsProcessed);
      expect(insertParams[5]).toBe(validEvent.recordsInserted);
      expect(insertParams[6]).toBe(validEvent.recordsUpdated);
    });

    test('should use default 0 for optional numeric fields', async () => {
      const eventWithoutCounts = { ...validEvent, recordsInserted: undefined, recordsUpdated: undefined, processingTime: undefined };
      await consumer.processDataUpdateEvent(eventWithoutCounts);

      const insertParams = database.executeQuery.mock.calls[0][1];
      expect(insertParams[5]).toBe(0); // recordsInserted default
      expect(insertParams[6]).toBe(0); // recordsUpdated default
      expect(insertParams[7]).toBe(0); // processingTime default
    });

    test('should auto-create upload_events table and retry if table does not exist', async () => {
      database.executeQuery
        .mockRejectedValueOnce(new Error('relation "upload_events" does not exist'))
        .mockResolvedValue({ rows: [] }); // subsequent calls succeed

      await consumer.processDataUpdateEvent(validEvent);

      // First call fails -> createUploadEventsTable -> retry INSERT -> UPDATE
      expect(database.executeQuery.mock.calls.length).toBeGreaterThanOrEqual(3);
    });

    test('should throw when a non-table-missing DB error occurs', async () => {
      database.executeQuery.mockRejectedValueOnce(new Error('permission denied'));

      await expect(consumer.processDataUpdateEvent(validEvent)).rejects.toThrow('permission denied');
    });
  });

  // ── updateCache() ────────────────────────────────────────────────────────

  describe('updateCache()', () => {
    const eventData = { uploadId: 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee' };

    test('should delete all_records and record_stats from Redis when healthy', async () => {
      redisService.isHealthy.mockReturnValue(true);

      await consumer.updateCache(eventData);

      expect(redisService.del).toHaveBeenCalledWith('all_records');
      expect(redisService.del).toHaveBeenCalledWith('record_stats');
      expect(redisService.del).toHaveBeenCalledTimes(2);
    });

    test('should skip cache deletion when Redis is not healthy', async () => {
      redisService.isHealthy.mockReturnValue(false);

      await consumer.updateCache(eventData);

      expect(redisService.del).not.toHaveBeenCalled();
    });

    test('should not throw when redisService.del throws (non-critical)', async () => {
      redisService.isHealthy.mockReturnValue(true);
      redisService.del.mockRejectedValueOnce(new Error('Redis write error'));

      await expect(consumer.updateCache(eventData)).resolves.not.toThrow();
    });
  });

  // ── getServiceStatus() ───────────────────────────────────────────────────

  describe('getServiceStatus()', () => {
    test('should return correct status when consumer has not started', async () => {
      const status = await consumer.getServiceStatus();

      expect(status.serviceName).toBe('Kafka Consumer Service');
      expect(status.isRunning).toBe(false);
      expect(status.startTime).toBeNull();
      expect(status.uptime).toBe(0);
      expect(status.processedMessages).toBe(0);
      expect(status.errorCount).toBe(0);
      expect(status.successRate).toBe('0%');
    });

    test('should return correct status after processing messages', async () => {
      consumer.isRunning = true;
      consumer.startTime = new Date(Date.now() - 5000);
      consumer.processedMessages = 9;
      consumer.errorCount = 1;

      const status = await consumer.getServiceStatus();

      expect(status.isRunning).toBe(true);
      expect(status.processedMessages).toBe(9);
      expect(status.errorCount).toBe(1);
      expect(status.successRate).toBe('90.00%');
      expect(status.uptime).toBeGreaterThan(0);
    });

    test('should report redis and kafka health correctly', async () => {
      redisService.isHealthy.mockReturnValue(true);
      kafkaService.isHealthy.mockReturnValue(true);

      const status = await consumer.getServiceStatus();

      expect(status.services.redis).toBe('connected');
      expect(status.services.kafka).toBe('connected');
    });

    test('should report redis and kafka as disconnected when unhealthy', async () => {
      redisService.isHealthy.mockReturnValue(false);
      kafkaService.isHealthy.mockReturnValue(false);

      const status = await consumer.getServiceStatus();

      expect(status.services.redis).toBe('disconnected');
      expect(status.services.kafka).toBe('disconnected');
    });

    test('should include kafka config in status', async () => {
      const status = await consumer.getServiceStatus();

      expect(status.config).toHaveProperty('topic');
      expect(status.config).toHaveProperty('groupId');
      expect(status.config).toHaveProperty('brokers');
    });
  });

  // ── End-to-end flow through handleMessage ────────────────────────────────

  describe('End-to-end message flow (Upload → DB → Cache)', () => {
    test('should execute full flow: parse → validate → DB insert → cache clear', async () => {
      redisService.isHealthy.mockReturnValue(true);

      const message = buildValidMessage({
        recordsInserted: 5,
        recordsUpdated: 1
      });

      await consumer.handleMessage(message);

      // DB was written to
      expect(database.executeQuery).toHaveBeenCalled();

      // Cache was cleared
      expect(redisService.del).toHaveBeenCalledWith('all_records');
      expect(redisService.del).toHaveBeenCalledWith('record_stats');

      // Message counted as success
      expect(consumer.processedMessages).toBe(1);
      expect(consumer.errorCount).toBe(0);
    });

    test('should clear cache even when recordsInserted/Updated are zero (re-upload scenario)', async () => {
      const message = buildValidMessage({ recordsInserted: 0, recordsUpdated: 0 });

      await consumer.handleMessage(message);

      expect(consumer.processedMessages).toBe(1);
      expect(redisService.del).toHaveBeenCalledWith('all_records');
    });

    test('should not clear cache when Redis is unavailable but still count message as processed', async () => {
      redisService.isHealthy.mockReturnValue(false);

      const message = buildValidMessage();
      await consumer.handleMessage(message);

      expect(consumer.processedMessages).toBe(1);
      expect(redisService.del).not.toHaveBeenCalled();
    });

    test('should handle duplicate messages without crashing (idempotency)', async () => {
      const message = buildValidMessage();

      // Process same message twice
      await consumer.handleMessage(message);
      await consumer.handleMessage(message);

      expect(consumer.processedMessages).toBe(2);
      expect(consumer.errorCount).toBe(0);
    });
  });

});
