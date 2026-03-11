const request = require('supertest');
const { App } = require('../src/app');
const database = require('../src/database/connection');
const redisService = require('../src/services/redisService');
const kafkaService = require('../src/services/kafkaService');

describe('InstaSupply Data Pipeline API', () => {
  let app;
  let server;

  beforeAll(async () => {
    // Initialize application for testing
    const appInstance = new App();
    app = await appInstance.initialize();
    
    // Don't start the server, supertest will handle that
    // server = await appInstance.start();
  }, 30000);

  afterAll(async () => {
    // Cleanup connections
    try {
      if (server) {
        server.close();
      }
      await database.disconnect();
      if (redisService.isHealthy()) {
        await redisService.disconnect();
      }
      await kafkaService.disconnect();
    } catch (error) {
      console.warn('Cleanup error:', error.message);
    }
  });

  describe('Health Endpoints', () => {
    test('GET / should return service information', async () => {
      const response = await request(app)
        .get('/')
        .expect(200);

      expect(response.body).toHaveProperty('service');
      expect(response.body).toHaveProperty('version');
      expect(response.body).toHaveProperty('status', 'running');
      expect(response.body).toHaveProperty('endpoints');
    });

    test('GET /api/health should return health status', async () => {
      const response = await request(app)
        .get('/api/health')
        .expect((res) => {
          expect([200, 503]).toContain(res.status);
        });

      expect(response.body).toHaveProperty('status');
      expect(response.body).toHaveProperty('services');
      expect(response.body.services).toHaveProperty('database');
      expect(response.body.services).toHaveProperty('redis');
      expect(response.body.services).toHaveProperty('kafka');
    });

    test('GET /api/ready should return readiness status', async () => {
      const response = await request(app)
        .get('/api/ready')
        .expect((res) => {
          expect([200, 503]).toContain(res.status);
        });

      expect(response.body).toHaveProperty('status');
    });
  });

  describe('Data Endpoints', () => {
    test('GET /api/data should return records with metadata', async () => {
      const response = await request(app)
        .get('/api/data')
        .expect(200);

      expect(response.body).toHaveProperty('success', true);
      expect(response.body).toHaveProperty('data');
      expect(response.body).toHaveProperty('metadata');
      expect(Array.isArray(response.body.data)).toBe(true);
      expect(response.body.metadata).toHaveProperty('count');
      expect(response.body.metadata).toHaveProperty('source');
      expect(response.body.metadata).toHaveProperty('responseTime');
    });

    test('GET /api/data with pagination should return paginated results', async () => {
      const response = await request(app)
        .get('/api/data?page=1&limit=5')
        .expect(200);

      expect(response.body).toHaveProperty('success', true);
      expect(response.body.data.length).toBeLessThanOrEqual(5);
      expect(response.body.metadata).toHaveProperty('pagination');
      expect(response.body.metadata.pagination).toHaveProperty('page', 1);
      expect(response.body.metadata.pagination).toHaveProperty('limit', 5);
    });

    test('GET /api/data/stats should return statistics', async () => {
      const response = await request(app)
        .get('/api/data/stats')
        .expect(200);

      expect(response.body).toHaveProperty('success', true);
      expect(response.body).toHaveProperty('data');
      expect(response.body.data).toHaveProperty('totalRecords');
      expect(typeof response.body.data.totalRecords).toBe('number');
    });

    test('GET /api/data/cache/status should return cache status', async () => {
      const response = await request(app)
        .get('/api/data/cache/status')
        .expect(200);

      expect(response.body).toHaveProperty('success', true);
      expect(response.body).toHaveProperty('cacheStatus');
      expect(response.body.cacheStatus).toHaveProperty('redisHealthy');
      expect(response.body.cacheStatus).toHaveProperty('cacheExists');
    });
  });

  describe('Upload Endpoints', () => {
    test('POST /api/upload without file should return error', async () => {
      const response = await request(app)
        .post('/api/upload')
        .expect(400);

      expect(response.body).toHaveProperty('success', false);
      expect(response.body).toHaveProperty('error');
      expect(response.body.error).toContain('No file uploaded');
    });

    test('GET /api/upload/history should return upload history', async () => {
      const response = await request(app)
        .get('/api/upload/history')
        .expect(200);

      expect(response.body).toHaveProperty('success', true);
      expect(response.body).toHaveProperty('data');
      expect(response.body).toHaveProperty('count');
      expect(Array.isArray(response.body.data)).toBe(true);
    });

    test('GET /api/upload/:uploadId should handle non-existent ID', async () => {
      const response = await request(app)
        .get('/api/upload/non-existent-id')
        .expect(404);

      expect(response.body).toHaveProperty('success', false);
      expect(response.body).toHaveProperty('error');
    });
  });

  describe('Error Handling', () => {
    test('GET /non-existent-endpoint should return 404', async () => {
      const response = await request(app)
        .get('/non-existent-endpoint')
        .expect(404);

      expect(response.body).toHaveProperty('success', false);
      expect(response.body).toHaveProperty('error', 'Endpoint not found');
      expect(response.body).toHaveProperty('availableEndpoints');
    });

    test('POST /api/data should return 404 (method not allowed)', async () => {
      const response = await request(app)
        .post('/api/data')
        .expect(404);

      expect(response.body).toHaveProperty('success', false);
    });
  });

  describe('Cache Operations', () => {
    test('DELETE /api/data/cache should clear cache', async () => {
      const response = await request(app)
        .delete('/api/data/cache')
        .expect(200);

      expect(response.body).toHaveProperty('success', true);
      expect(response.body).toHaveProperty('message');
    });
  });
});