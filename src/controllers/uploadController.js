const multer = require('multer');
const path = require('path');
const fs = require('fs').promises;
const dataService = require('../services/dataService');
const kafkaService = require('../services/kafkaService');
const logger = require('../utils/logger');
const config = require('../config/config');

// Configure multer for file uploads
const storage = multer.diskStorage({
  destination: async (req, file, cb) => {
    const uploadDir = path.resolve(config.upload.dir);
    try {
      await fs.mkdir(uploadDir, { recursive: true });
      cb(null, uploadDir);
    } catch (error) {
      cb(error);
    }
  },
  filename: (req, file, cb) => {
    // Generate unique filename with timestamp
    const timestamp = Date.now();
    const originalName = file.originalname.replace(/[^a-zA-Z0-9.-]/g, '_');
    cb(null, `${timestamp}_${originalName}`);
  }
});

const fileFilter = (req, file, cb) => {
  // Check file type
  const allowedMimes = config.upload.allowedTypes;
  const isValidType = allowedMimes.includes(file.mimetype) || 
                     file.originalname.toLowerCase().endsWith('.csv');
  
  if (isValidType) {
    cb(null, true);
  } else {
    cb(new Error(`Invalid file type. Allowed types: ${allowedMimes.join(', ')}`), false);
  }
};

const upload = multer({
  storage,
  fileFilter,
  limits: {
    fileSize: config.upload.maxFileSize,
    files: 1
  }
});

class UploadController {
  constructor() {
    this.uploadSingle = upload.single('csvFile');
  }

  async uploadCSV(req, res) {
    const startTime = Date.now();
    
    try {
      // Handle file upload with multer
      await new Promise((resolve, reject) => {
        this.uploadSingle(req, res, (error) => {
          if (error) {
            if (error instanceof multer.MulterError) {
              if (error.code === 'LIMIT_FILE_SIZE') {
                return reject({ status: 400, message: 'File too large' });
              }
              if (error.code === 'LIMIT_UNEXPECTED_FILE') {
                return reject({ status: 400, message: 'Unexpected field name. Use "csvFile"' });
              }
            }
            return reject({ status: 400, message: error.message });
          }
          resolve();
        });
      });

      // Validate file upload
      if (!req.file) {
        return res.status(400).json({
          success: false,
          error: 'No file uploaded',
          message: 'Please upload a CSV file using the "csvFile" field'
        });
      }

      const { originalname, filename, path: filePath, size } = req.file;

      logger.info('File upload received:', {
        originalname,
        filename,
        size,
        filePath
      });

      // Process the CSV file
      const processingResult = await dataService.processCSVFile(filePath, originalname);

      // Publish to Kafka if processing was successful
      if (processingResult.status === 'success') {
        try {
          await kafkaService.publishDataUpdate({
            uploadId: processingResult.uploadId,
            filename: originalname,
            recordsProcessed: processingResult.summary.validRows,
            recordsInserted: processingResult.summary.recordsInserted,
            recordsUpdated: processingResult.summary.recordsUpdated,
            processingTime: processingResult.processingTime
          });
        } catch (kafkaError) {
          logger.error('Failed to publish to Kafka:', kafkaError);
          // Don't fail the entire operation if Kafka is down
        }
      }

      // Clean up uploaded file
      try {
        await fs.unlink(filePath);
        logger.debug('Temporary file cleaned up:', filePath);
      } catch (cleanupError) {
        logger.warn('Failed to clean up temporary file:', cleanupError);
      }

      const totalTime = Date.now() - startTime;

      // Return response based on processing result
      if (processingResult.status === 'duplicate') {
        return res.status(200).json({
          success: true,
          status: 'duplicate',
          message: processingResult.message,
          uploadId: processingResult.uploadId,
          existingUpload: processingResult.existingUpload,
          processingTime: totalTime
        });
      }

      // Success response
      res.status(200).json({
        success: true,
        message: 'CSV file processed successfully',
        uploadId: processingResult.uploadId,
        summary: processingResult.summary,
        invalidRows: processingResult.invalidRows.length > 0 ? {
          count: processingResult.invalidRows.length,
          examples: processingResult.invalidRows.slice(0, 5) // Show first 5 invalid rows
        } : null,
        processingTime: totalTime
      });

    } catch (error) {
      logger.error('Upload processing failed:', error);

      // Clean up file if it exists
      if (req.file?.path) {
        try {
          await fs.unlink(req.file.path);
        } catch (cleanupError) {
          logger.warn('Failed to clean up file after error:', cleanupError);
        }
      }

      const statusCode = error.status || 500;
      const message = error.message || 'Internal server error';

      res.status(statusCode).json({
        success: false,
        error: message,
        uploadId: null,
        processingTime: Date.now() - startTime
      });
    }
  }

  async getUploadHistory(req, res) {
    try {
      const limit = parseInt(req.query.limit) || 50;
      const history = await dataService.getUploadHistory(limit);

      res.status(200).json({
        success: true,
        data: history,
        count: history.length
      });

    } catch (error) {
      logger.error('Failed to get upload history:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to retrieve upload history'
      });
    }
  }

  async getUploadStatus(req, res) {
    try {
      const { uploadId } = req.params;

      if (!uploadId) {
        return res.status(400).json({
          success: false,
          error: 'Upload ID is required'
        });
      }

      const history = await dataService.getUploadHistory(100);
      const upload = history.find(h => h.id === uploadId);

      if (!upload) {
        return res.status(404).json({
          success: false,
          error: 'Upload not found'
        });
      }

      res.status(200).json({
        success: true,
        data: upload
      });

    } catch (error) {
      logger.error('Failed to get upload status:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to retrieve upload status'
      });
    }
  }
}

module.exports = new UploadController();