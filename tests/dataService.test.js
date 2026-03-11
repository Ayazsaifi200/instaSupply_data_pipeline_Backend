const dataService = require('../src/services/dataService');
const database = require('../src/database/connection');
const fs = require('fs');
const path = require('path');

describe('DataService', () => {
  beforeAll(async () => {
    // Initialize database connection
    await database.connect();
  });

  afterAll(async () => {
    // Clean up
    await database.disconnect();
  });

  describe('CSV Processing', () => {
    const testCsvPath = path.join(__dirname, 'test-data.csv');

    beforeEach(async () => {
      // Create a test CSV file
      const csvContent = `name,email,age,city,country,salary,department
John Doe,john.doe@test.com,30,New York,USA,75000,Engineering
Jane Smith,jane.smith@test.com,28,Los Angeles,USA,68000,Marketing`;
      
      fs.writeFileSync(testCsvPath, csvContent);

      // Clean up any prior upload_logs so duplicate detection doesn't interfere
      await database.executeQuery(
        "DELETE FROM upload_logs WHERE original_filename IN ('test-data.csv', 'invalid-data.csv', 'mixed-data.csv', 'empty.csv')"
      );
    });

    afterEach(() => {
      // Clean up test file
      if (fs.existsSync(testCsvPath)) {
        fs.unlinkSync(testCsvPath);
      }
    });

    test('should process valid CSV file', async () => {
      const result = await dataService.processCSVFile(testCsvPath, 'test-data.csv');

      expect(result).toHaveProperty('status', 'success');
      expect(result).toHaveProperty('uploadId');
      expect(result).toHaveProperty('summary');
      expect(result.summary.totalRows).toBe(2);
      expect(result.summary.validRows).toBe(2);
      expect(result.summary.invalidRows).toBe(0);
    }, 10000);

    test('should handle CSV with missing required fields', async () => {
      const csvContent = `name,age,city
John Doe,30,New York
Jane Smith,28,Los Angeles`;
      
      fs.writeFileSync(testCsvPath, csvContent);

      await expect(dataService.processCSVFile(testCsvPath, 'invalid-data.csv'))
        .rejects.toThrow('CSV must contain at least "name" and "email" columns');
    });

    test('should validate row data correctly', async () => {
      const csvContent = `name,email,age,city,country,salary,department
John Doe,john.doe@test.com,30,New York,USA,75000,Engineering
Invalid User,invalid-email,thirty,Los Angeles,USA,invalid-salary,Marketing`;
      
      fs.writeFileSync(testCsvPath, csvContent);

      const result = await dataService.processCSVFile(testCsvPath, 'mixed-data.csv');

      expect(result.status).toBe('success');
      expect(result.summary.validRows).toBe(1);
      expect(result.summary.invalidRows).toBe(1);
      expect(result.invalidRows).toHaveLength(1);
    }, 10000);
  });

  describe('Data Retrieval', () => {
    test('should retrieve all records', async () => {
      const records = await dataService.getAllRecords();
      
      expect(Array.isArray(records)).toBe(true);
      // Check if each record has expected properties
      if (records.length > 0) {
        expect(records[0]).toHaveProperty('id');
        expect(records[0]).toHaveProperty('name');
        expect(records[0]).toHaveProperty('email');
        expect(records[0]).toHaveProperty('created_at');
      }
    });

    test('should get record count', async () => {
      const count = await dataService.getRecordCount();
      
      expect(typeof count).toBe('number');
      expect(count).toBeGreaterThanOrEqual(0);
    });

    test('should get upload history', async () => {
      const history = await dataService.getUploadHistory(10);
      
      expect(Array.isArray(history)).toBe(true);
      expect(history.length).toBeLessThanOrEqual(10);
      
      if (history.length > 0) {
        expect(history[0]).toHaveProperty('id');
        expect(history[0]).toHaveProperty('original_filename');
        expect(history[0]).toHaveProperty('status');
        expect(history[0]).toHaveProperty('upload_time');
      }
    });
  });

  describe('Utility Methods', () => {
    test('should calculate file hash', async () => {
      const testFile = path.join(__dirname, 'hash-test.txt');
      fs.writeFileSync(testFile, 'test content for hashing');

      const hash1 = await dataService.calculateFileHash(testFile);
      const hash2 = await dataService.calculateFileHash(testFile);

      expect(hash1).toBe(hash2);
      expect(hash1).toHaveLength(64); // SHA256 hash length
      expect(typeof hash1).toBe('string');

      // Clean up
      fs.unlinkSync(testFile);
    });

    test('should normalize row data correctly', () => {
      const testRow = {
        'Full Name': 'John Doe',
        'Email Address': 'john@example.com',
        'Years': '30',
        'Annual Salary': '$75,000'
      };

      const normalized = dataService.normalizeRowData(testRow, Object.keys(testRow));

      expect(normalized).toHaveProperty('name', 'John Doe');
      expect(normalized).toHaveProperty('email', 'john@example.com');
      expect(normalized).toHaveProperty('age', 30);
      expect(normalized).toHaveProperty('salary', 75000);
    });
  });

  describe('Error Handling', () => {
    test('should handle non-existent file', async () => {
      await expect(dataService.processCSVFile('/non/existent/file.csv', 'test.csv'))
        .rejects.toThrow();
    });

    test('should handle empty CSV file', async () => {
      const emptyFile = path.join(__dirname, 'empty.csv');
      fs.writeFileSync(emptyFile, '');

      await expect(dataService.processCSVFile(emptyFile, 'empty.csv'))
        .rejects.toThrow();

      // Clean up
      fs.unlinkSync(emptyFile);
    });
  });
});