const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const compression = require('compression');
const path = require('path');

// Import configuration and utilities
const config = require('./config/config');
const logger = require('./utils/logger');
const database = require('./database/connection');
const redisService = require('./services/redisService');
const kafkaService = require('./services/kafkaService');

// Import routes
const uploadRoutes = require('./routes/upload');
const dataRoutes = require('./routes/data');
const healthRoutes = require('./routes/health');

class App {
  constructor() {
    this.app = express();
    this.isShuttingDown = false;
  }

  async initialize() {
    try {
      // Setup middleware
      this.setupMiddleware();
      
      // Setup routes
      this.setupRoutes();
      
      // Setup error handling
      this.setupErrorHandling();
      
      // Initialize services
      await this.initializeServices();
      
      // Setup graceful shutdown
      this.setupGracefulShutdown();
      
      logger.info('Application initialized successfully');
      return this.app;
    } catch (error) {
      logger.error('Application initialization failed:', error);
      throw error;
    }
  }

  setupMiddleware() {
    // Security middleware
    this.app.use(helmet({
      crossOriginResourcePolicy: { policy: "cross-origin" }
    }));
    
    // Compression
    this.app.use(compression());
    
    // CORS
    this.app.use(cors({
      origin: process.env.ALLOWED_ORIGINS?.split(',') || '*',
      credentials: true
    }));
    
    // Body parsing
    this.app.use(express.json({ limit: '10mb' }));
    this.app.use(express.urlencoded({ extended: true, limit: '10mb' }));
    
    // Request logging
    this.app.use((req, res, next) => {
      const start = Date.now();
      
      res.on('finish', () => {
        const duration = Date.now() - start;
        logger.info('Request processed:', {
          method: req.method,
          url: req.url,
          statusCode: res.statusCode,
          duration: `${duration}ms`,
          userAgent: req.get('user-agent'),
          ip: req.ip
        });
      });
      
      next();
    });

    // Request timeout
    this.app.use((req, res, next) => {
      res.setTimeout(30000, () => {
        logger.warn('Request timeout:', {
          method: req.method,
          url: req.url,
          ip: req.ip
        });
        res.status(408).json({
          success: false,
          error: 'Request timeout'
        });
      });
      next();
    });
  }

  setupRoutes() {
    // API routes
    this.app.use('/api/upload', uploadRoutes);
    this.app.use('/api/data', dataRoutes);
    this.app.use('/api', healthRoutes);
    
    // Root endpoint
    this.app.get('/', (req, res) => {
      res.json({
        service: 'InstaSupply Data Pipeline API',
        version: '1.0.0',
        status: 'running',
        timestamp: new Date().toISOString(),
        endpoints: {
          upload: '/api/upload',
          data: '/api/data',
          health: '/api/health',
          ready: '/api/ready'
        },
        documentation: {
          upload: 'POST /api/upload - Upload CSV file',
          fetch: 'GET /api/data - Fetch all records (cached)',
          stats: 'GET /api/data/stats - Get record statistics',
          health: 'GET /api/health - Service health check'
        }
      });
    });
    
    // 404 handler
    this.app.use((req, res) => {
      res.status(404).json({
        success: false,
        error: 'Endpoint not found',
        path: req.path,
        method: req.method,
        availableEndpoints: [
          'POST /api/upload',
          'GET /api/data',
          'GET /api/data/stats',
          'GET /api/health'
        ]
      });
    });
  }

  setupErrorHandling() {
    // Global error handler
    this.app.use((error, req, res, next) => {
      logger.error('Unhandled error:', {
        error: error.message,
        stack: error.stack,
        method: req.method,
        url: req.url,
        body: req.body,
        params: req.params,
        query: req.query
      });

      // Don't send error details in production
      const isDevelopment = config.server.nodeEnv === 'development';
      
      res.status(error.status || 500).json({
        success: false,
        error: error.message || 'Internal server error',
        ...(isDevelopment && { stack: error.stack }),
        timestamp: new Date().toISOString()
      });
    });
  }

  async initializeServices() {
    try {
      // Initialize database
      await database.connect();
      logger.info('Database connection established');

      // Initialize Redis
      try {
        await redisService.connect();
        logger.info('Redis connection established');
      } catch (error) {
        logger.warn('Redis connection failed, continuing without cache:', error.message);
      }

      // Initialize Kafka producer
      try {
        await kafkaService.initializeProducer();
        logger.info('Kafka producer initialized');
      } catch (error) {
        logger.warn('Kafka producer initialization failed, continuing without messaging:', error.message);
      }

    } catch (error) {
      logger.error('Critical service initialization failed:', error);
      throw error;
    }
  }

  setupGracefulShutdown() {
    const shutdown = async (signal) => {
      if (this.isShuttingDown) {
        logger.warn(`Received ${signal} but already shutting down`);
        return;
      }

      this.isShuttingDown = true;
      logger.info(`Received ${signal}, starting graceful shutdown`);

      // Stop accepting new requests
      this.server?.close((error) => {
        if (error) {
          logger.error('Error during server shutdown:', error);
        } else {
          logger.info('HTTP server closed');
        }
      });

      try {
        // Close database connections
        await database.disconnect();
        
        // Close Redis connection
        await redisService.disconnect();
        
        // Close Kafka connections
        await kafkaService.disconnect();
        
        logger.info('All services disconnected gracefully');
        process.exit(0);
      } catch (error) {
        logger.error('Error during graceful shutdown:', error);
        process.exit(1);
      }
    };

    // Handle shutdown signals
    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));
    
    // Handle uncaught exceptions
    process.on('uncaughtException', (error) => {
      logger.error('Uncaught exception:', error);
      shutdown('uncaughtException');
    });
    
    process.on('unhandledRejection', (reason, promise) => {
      logger.error('Unhandled rejection:', { reason, promise });
      shutdown('unhandledRejection');
    });
  }

  async start() {
    const port = config.server.port;
    
    this.server = this.app.listen(port, () => {
      logger.info(`Server started on port ${port}`, {
        environment: config.server.nodeEnv,
        port,
        pid: process.pid
      });
    });

    return this.server;
  }
}

// Initialize and start the application
async function startApplication() {
  try {
    const app = new App();
    await app.initialize();
    await app.start();
  } catch (error) {
    logger.error('Failed to start application:', error);
    process.exit(1);
  }
}

// Start the application if this file is run directly
if (require.main === module) {
  startApplication();
}

module.exports = { App, startApplication };