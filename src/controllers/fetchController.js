const dataService = require('../services/dataService');
const redisService = require('../services/redisService');
const logger = require('../utils/logger');
const config = require('../config/config');

class FetchController {
  constructor() {
    this.cacheKey = 'all_records';
  }

  async getAllRecords(req, res) {
    const startTime = Date.now();
    let dataSource = 'database'; // Track where data came from

    try {
      // Try to get data from Redis cache first
      let records = await redisService.get(this.cacheKey);
      
      if (records) {
        dataSource = 'cache';
        logger.debug('Data served from Redis cache');
      } else {
        // Cache miss or Redis unavailable - fallback to database
        logger.debug('Cache miss or Redis unavailable, fetching from database');
        
        records = await dataService.getAllRecords();
        
        // Try to update cache for next request (fire and forget)
        this.updateCacheAsync(records);
        dataSource = 'database';
      }

      const responseTime = Date.now() - startTime;
      const recordCount = records.length;

      // Add metadata to response
      const response = {
        success: true,
        data: records,
        metadata: {
          count: recordCount,
          source: dataSource,
          responseTime: `${responseTime}ms`,
          cacheStatus: redisService.isHealthy() ? 'available' : 'unavailable',
          timestamp: new Date().toISOString()
        }
      };

      // Add pagination info if query params are provided
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || null;
      
      if (limit && limit > 0) {
        const offset = (page - 1) * limit;
        const paginatedRecords = records.slice(offset, offset + limit);
        const totalPages = Math.ceil(recordCount / limit);
        
        response.data = paginatedRecords;
        response.metadata.pagination = {
          page,
          limit,
          totalRecords: recordCount,
          totalPages,
          hasNext: page < totalPages,
          hasPrevious: page > 1
        };
      }

      logger.info('Records fetched successfully:', {
        count: recordCount,
        source: dataSource,
        responseTime,
        pagination: response.metadata.pagination || null
      });

      res.status(200).json(response);

    } catch (error) {
      const responseTime = Date.now() - startTime;
      
      logger.error('Failed to fetch records:', {
        error: error.message,
        responseTime,
        dataSource
      });

      res.status(500).json({
        success: false,
        error: 'Failed to retrieve records',
        metadata: {
          source: dataSource,
          responseTime: `${responseTime}ms`,
          cacheStatus: redisService.isHealthy() ? 'available' : 'unavailable',
          timestamp: new Date().toISOString()
        }
      });
    }
  }

  async getRecordById(req, res) {
    const startTime = Date.now();
    
    try {
      const { id } = req.params;
      
      if (!id) {
        return res.status(400).json({
          success: false,
          error: 'Record ID is required'
        });
      }

      // For individual records, we could implement per-record caching
      // but for simplicity, we'll query the database directly
      const query = 'SELECT * FROM data_records WHERE id = $1';
      const result = await require('../database/connection').executeQuery(query, [id]);
      
      if (result.rows.length === 0) {
        return res.status(404).json({
          success: false,
          error: 'Record not found'
        });
      }

      const responseTime = Date.now() - startTime;

      res.status(200).json({
        success: true,
        data: result.rows[0],
        metadata: {
          source: 'database',
          responseTime: `${responseTime}ms`,
          timestamp: new Date().toISOString()
        }
      });

    } catch (error) {
      const responseTime = Date.now() - startTime;
      
      logger.error('Failed to fetch record by ID:', {
        error: error.message,
        recordId: req.params.id,
        responseTime
      });

      res.status(500).json({
        success: false,
        error: 'Failed to retrieve record',
        metadata: {
          responseTime: `${responseTime}ms`,
          timestamp: new Date().toISOString()
        }
      });
    }
  }

  async getRecordStats(req, res) {
    const startTime = Date.now();
    
    try {
      const statsKey = 'record_stats';
      
      // Try cache first
      let stats = await redisService.get(statsKey);
      let dataSource = 'cache';
      
      if (!stats) {
        // Calculate stats from database
        const queries = [
          'SELECT COUNT(*) as total_records FROM data_records',
          'SELECT COUNT(DISTINCT department) as unique_departments FROM data_records WHERE department IS NOT NULL',
          'SELECT COUNT(DISTINCT country) as unique_countries FROM data_records WHERE country IS NOT NULL',
          'SELECT AVG(age) as average_age FROM data_records WHERE age IS NOT NULL',
          'SELECT AVG(salary) as average_salary FROM data_records WHERE salary IS NOT NULL'
        ];
        
        const results = await Promise.all(queries.map(query => 
          require('../database/connection').executeQuery(query)
        ));
        
        stats = {
          totalRecords: parseInt(results[0].rows[0].total_records),
          uniqueDepartments: parseInt(results[1].rows[0].unique_departments),
          uniqueCountries: parseInt(results[2].rows[0].unique_countries),
          averageAge: parseFloat(results[3].rows[0].average_age) || 0,
          averageSalary: parseFloat(results[4].rows[0].average_salary) || 0,
          lastUpdated: new Date().toISOString()
        };
        
        // Cache for 5 minutes
        await redisService.set(statsKey, stats, 300);
        dataSource = 'database';
      }
      
      const responseTime = Date.now() - startTime;
      
      res.status(200).json({
        success: true,
        data: stats,
        metadata: {
          source: dataSource,
          responseTime: `${responseTime}ms`,
          cacheStatus: redisService.isHealthy() ? 'available' : 'unavailable',
          timestamp: new Date().toISOString()
        }
      });

    } catch (error) {
      const responseTime = Date.now() - startTime;
      
      logger.error('Failed to fetch record stats:', {
        error: error.message,
        responseTime
      });

      res.status(500).json({
        success: false,
        error: 'Failed to retrieve statistics',
        metadata: {
          responseTime: `${responseTime}ms`,
          timestamp: new Date().toISOString()
        }
      });
    }
  }

  async clearCache(req, res) {
    try {
      const cleared = await redisService.del(this.cacheKey);
      
      if (cleared) {
        logger.info('Cache cleared manually');
        res.status(200).json({
          success: true,
          message: 'Cache cleared successfully'
        });
      } else {
        res.status(200).json({
          success: true,
          message: 'Cache was already empty or Redis unavailable'
        });
      }
      
    } catch (error) {
      logger.error('Failed to clear cache:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to clear cache'
      });
    }
  }

  async getCacheStatus(req, res) {
    try {
      const cacheExists = await redisService.exists(this.cacheKey);
      const redisHealthy = redisService.isHealthy();
      
      res.status(200).json({
        success: true,
        cacheStatus: {
          redisHealthy,
          cacheExists,
          cacheKey: this.cacheKey
        }
      });
      
    } catch (error) {
      logger.error('Failed to get cache status:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to get cache status'
      });
    }
  }

  // Private method to update cache asynchronously
  async updateCacheAsync(records) {
    try {
      await redisService.set(this.cacheKey, records, config.redis.ttl);
      logger.debug('Cache updated with fresh data');
    } catch (error) {
      logger.warn('Failed to update cache (non-critical):', error.message);
    }
  }
}

module.exports = new FetchController();