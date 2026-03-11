const csv = require('csv-parser');
const fs = require('fs');
const crypto = require('crypto');
const { v4: uuidv4 } = require('uuid');
const Joi = require('joi');
const database = require('../database/connection');
const logger = require('../utils/logger');

// Validation schema for CSV data
const csvRowSchema = Joi.object({
  name: Joi.string().trim().min(1).max(255).required(),
  email: Joi.string().email().trim().max(255).required(),
  age: Joi.number().integer().min(0).max(150).optional(),
  city: Joi.string().trim().max(255).optional().allow(''),
  country: Joi.string().trim().max(255).optional().allow(''),
  salary: Joi.number().precision(2).min(0).optional(),
  department: Joi.string().trim().max(255).optional().allow('')
});

class DataService {
  constructor() {
    this.supportedColumns = ['name', 'email', 'age', 'city', 'country', 'salary', 'department'];
  }

  async processCSVFile(filePath, originalFilename) {
    const uploadId = uuidv4();
    const startTime = Date.now();
    
    try {
      logger.info('Starting CSV processing:', { uploadId, originalFilename });

      // Calculate file hash for duplicate detection
      const fileHash = await this.calculateFileHash(filePath);
      
      // Check if this file was already processed
      const existingUpload = await this.checkDuplicateUpload(fileHash);
      if (existingUpload) {
        logger.info('Duplicate file detected:', { uploadId, fileHash, existingUploadId: existingUpload.id });
        return {
          uploadId,
          status: 'duplicate',
          message: 'This file has already been processed',
          existingUpload,
          processingTime: Date.now() - startTime
        };
      }

      // Parse and validate CSV
      const { validRows, invalidRows, totalRows } = await this.parseAndValidateCSV(filePath);
      
      if (validRows.length === 0) {
        throw new Error('No valid records found in CSV file');
      }

      // Process data in batches
      const { inserted, updated } = await this.batchProcessRecords(validRows);
      
      const processingTime = Date.now() - startTime;

      // Log upload attempt
      await this.logUpload({
        uploadId,
        originalFilename,
        fileHash,
        recordsProcessed: totalRows,
        recordsInserted: inserted,
        recordsUpdated: updated,
        status: 'success',
        processingTime
      });

      logger.info('CSV processing completed:', {
        uploadId,
        totalRows,
        validRows: validRows.length,
        invalidRows: invalidRows.length,
        inserted,
        updated,
        processingTime
      });

      return {
        uploadId,
        status: 'success',
        summary: {
          totalRows,
          validRows: validRows.length,
          invalidRows: invalidRows.length,
          recordsInserted: inserted,
          recordsUpdated: updated
        },
        invalidRows,
        processingTime
      };

    } catch (error) {
      const processingTime = Date.now() - startTime;
      
      // Log failed upload
      await this.logUpload({
        uploadId,
        originalFilename,
        fileHash: await this.calculateFileHash(filePath).catch(() => null),
        recordsProcessed: 0,
        recordsInserted: 0,
        recordsUpdated: 0,
        status: 'failed',
        errorMessage: error.message,
        processingTime
      });

      logger.error('CSV processing failed:', { uploadId, error: error.message });
      throw error;
    }
  }

  async parseAndValidateCSV(filePath) {
    return new Promise((resolve, reject) => {
      const validRows = [];
      const invalidRows = [];
      let totalRows = 0;
      let headerProcessed = false;
      let detectedColumns = [];

      const stream = fs.createReadStream(filePath)
        .pipe(csv({
          skipEmptyLines: true,
          strict: false
        }));

      stream.on('headers', (headers) => {
        detectedColumns = headers.map(h => h.trim().toLowerCase());
        logger.debug('CSV headers detected:', detectedColumns);
        
        // Check if we have at least required columns (name, email)
        const hasName = detectedColumns.some(col => col.includes('name'));
        const hasEmail = detectedColumns.some(col => col.includes('email'));
        
        if (!hasName || !hasEmail) {
          return reject(new Error('CSV must contain at least "name" and "email" columns'));
        }
        
        headerProcessed = true;
      });

      stream.on('data', (row) => {
        totalRows++;
        
        try {
          // Normalize column names and extract data
          const normalizedRow = this.normalizeRowData(row, detectedColumns);
          
          // Validate row data
          const { error, value } = csvRowSchema.validate(normalizedRow, { 
            abortEarly: false,
            stripUnknown: true,
            convert: true
          });

          if (error) {
            invalidRows.push({
              rowNumber: totalRows,
              data: row,
              errors: error.details.map(detail => detail.message)
            });
          } else {
            validRows.push(value);
          }
        } catch (error) {
          invalidRows.push({
            rowNumber: totalRows,
            data: row,
            errors: [error.message]
          });
        }
      });

      stream.on('end', () => {
        logger.debug('CSV parsing completed:', { totalRows, validRows: validRows.length, invalidRows: invalidRows.length });
        resolve({ validRows, invalidRows, totalRows });
      });

      stream.on('error', (error) => {
        logger.error('CSV parsing error:', error);
        reject(error);
      });
    });
  }

  normalizeRowData(row, detectedColumns) {
    const normalized = {};
    
    // Map detected columns to our schema
    Object.keys(row).forEach(key => {
      const normalizedKey = key.trim().toLowerCase();
      const value = row[key]?.toString().trim();
      
      // Map common column variations to our schema
      if (normalizedKey.includes('name')) {
        normalized.name = value;
      } else if (normalizedKey.includes('email')) {
        normalized.email = value;
      } else if (normalizedKey.includes('age') || normalizedKey === 'years' || normalizedKey === 'year') {
        normalized.age = value ? parseInt(value, 10) : null;
      } else if (normalizedKey.includes('city')) {
        normalized.city = value;
      } else if (normalizedKey.includes('country')) {
        normalized.country = value;
      } else if (normalizedKey.includes('salary') || normalizedKey.includes('wage')) {
        normalized.salary = value ? parseFloat(value.replace(/[$,]/g, '')) : null;
      } else if (normalizedKey.includes('department') || normalizedKey.includes('dept')) {
        normalized.department = value;
      }
    });

    return normalized;
  }

  async batchProcessRecords(records) {
    const batchSize = 100;
    let inserted = 0;
    let updated = 0;

    for (let i = 0; i < records.length; i += batchSize) {
      const batch = records.slice(i, i + batchSize);
      const { batchInserted, batchUpdated } = await this.processBatch(batch);
      inserted += batchInserted;
      updated += batchUpdated;
    }

    return { inserted, updated };
  }

  async processBatch(batch) {
    const queries = [];
    let batchInserted = 0;
    let batchUpdated = 0;

    for (const record of batch) {
      // Use ON CONFLICT to handle duplicate emails gracefully
      const query = `
        INSERT INTO data_records (name, email, age, city, country, salary, department)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (email) 
        DO UPDATE SET 
          name = EXCLUDED.name,
          age = EXCLUDED.age,
          city = EXCLUDED.city,
          country = EXCLUDED.country,
          salary = EXCLUDED.salary,
          department = EXCLUDED.department,
          updated_at = CURRENT_TIMESTAMP
        RETURNING (xmax = 0) AS inserted
      `;
      
      const params = [
        record.name,
        record.email,
        record.age || null,
        record.city || null,
        record.country || null,
        record.salary || null,
        record.department || null
      ];

      queries.push({ query, params });
    }

    try {
      const results = await database.executeTransaction(queries);
      
      // Count inserts vs updates
      results.forEach(result => {
        result.rows.forEach(row => {
          if (row.inserted) {
            batchInserted++;
          } else {
            batchUpdated++;
          }
        });
      });

      return { batchInserted, batchUpdated };
    } catch (error) {
      logger.error('Batch processing failed:', error);
      throw error;
    }
  }

  async getAllRecords() {
    try {
      const query = `
        SELECT id, name, email, age, city, country, salary, department, 
               created_at, updated_at
        FROM data_records 
        ORDER BY created_at DESC
      `;
      
      const result = await database.executeQuery(query);
      return result.rows;
    } catch (error) {
      logger.error('Failed to fetch all records:', error);
      throw error;
    }
  }

  async getRecordCount() {
    try {
      const result = await database.executeQuery('SELECT COUNT(*) as count FROM data_records');
      return parseInt(result.rows[0].count, 10);
    } catch (error) {
      logger.error('Failed to get record count:', error);
      throw error;
    }
  }

  async calculateFileHash(filePath) {
    return new Promise((resolve, reject) => {
      const hash = crypto.createHash('sha256');
      const stream = fs.createReadStream(filePath);
      
      stream.on('data', data => hash.update(data));
      stream.on('end', () => resolve(hash.digest('hex')));
      stream.on('error', reject);
    });
  }

  async checkDuplicateUpload(fileHash) {
    try {
      const query = "SELECT * FROM upload_logs WHERE file_hash = $1 AND status IN ('success', 'completed') ORDER BY upload_time DESC LIMIT 1";
      const result = await database.executeQuery(query, [fileHash]);
      return result.rows[0] || null;
    } catch (error) {
      logger.error('Failed to check duplicate upload:', error);
      return null;
    }
  }

  async logUpload(uploadData) {
    try {
      const query = `
        INSERT INTO upload_logs (
          id, original_filename, file_hash, records_processed, 
          records_inserted, records_updated, status, error_message, processing_time_ms
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      `;
      
      const params = [
        uploadData.uploadId,
        uploadData.originalFilename,
        uploadData.fileHash,
        uploadData.recordsProcessed,
        uploadData.recordsInserted,
        uploadData.recordsUpdated,
        uploadData.status,
        uploadData.errorMessage || null,
        uploadData.processingTime
      ];

      await database.executeQuery(query, params);
    } catch (error) {
      logger.error('Failed to log upload:', error);
      // Don't throw here as logging failure shouldn't break the main flow
    }
  }

  async getUploadHistory(limit = 50) {
    try {
      const query = `
        SELECT * FROM upload_logs 
        ORDER BY upload_time DESC 
        LIMIT $1
      `;
      
      const result = await database.executeQuery(query, [limit]);
      return result.rows;
    } catch (error) {
      logger.error('Failed to get upload history:', error);
      throw error;
    }
  }
}

module.exports = new DataService();