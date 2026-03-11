const express = require('express');
const database = require('../database/connection');
const redisService = require('../services/redisService');
const kafkaService = require('../services/kafkaService');
const logger = require('../utils/logger');

const router = express.Router();

router.get('/health', async (req, res) => {
  const health = {
    status: 'healthy',
    timestamp: new Date().toISOString(),
    services: {
      database: 'unknown',
      redis: 'unknown',
      kafka: 'unknown'
    },
    uptime: process.uptime(),
    memory: process.memoryUsage(),
    version: process.env.npm_package_version || '1.0.0'
  };

  try {
    // Check database
    try {
      await database.executeQuery('SELECT 1');
      health.services.database = 'healthy';
    } catch (error) {
      health.services.database = 'unhealthy';
      health.status = 'degraded';
    }

    // Check Redis
    health.services.redis = redisService.isHealthy() ? 'healthy' : 'unhealthy';
    if (health.services.redis === 'unhealthy') {
      health.status = 'degraded';
    }

    // Check Kafka
    health.services.kafka = kafkaService.isHealthy() ? 'healthy' : 'unhealthy';
    if (health.services.kafka === 'unhealthy') {
      health.status = 'degraded';
    }

    const statusCode = health.status === 'healthy' ? 200 : 503;
    res.status(statusCode).json(health);

  } catch (error) {
    logger.error('Health check failed:', error);
    res.status(500).json({
      status: 'unhealthy',
      timestamp: new Date().toISOString(),
      error: error.message
    });
  }
});

router.get('/ready', async (req, res) => {
  try {
    // Simple readiness check
    await database.executeQuery('SELECT 1');
    res.status(200).json({
      status: 'ready',
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(503).json({
      status: 'not ready',
      timestamp: new Date().toISOString(),
      error: error.message
    });
  }
});

module.exports = router;