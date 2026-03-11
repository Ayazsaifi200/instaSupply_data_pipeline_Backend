const database = require('./connection');
const logger = require('../utils/logger');
const fs = require('fs').promises;
const path = require('path');

async function setupDatabase() {
  try {
    logger.info('Setting up database...');
    
    // Connect to database
    await database.connect();
    
    // Read and execute init.sql
    const initSqlPath = path.join(__dirname, 'init.sql');
    const initSql = await fs.readFile(initSqlPath, 'utf8');
    
    // Execute the entire SQL file at once to handle complex queries properly
    await database.executeQuery(initSql);
    
    logger.info('Database setup completed successfully');
    
    // Test with a simple query
    const result = await database.executeQuery('SELECT COUNT(*) as count FROM data_records');
    logger.info(`Current records in database: ${result.rows[0].count}`);
    
  } catch (error) {
    logger.error('Database setup failed:', error);
    process.exit(1);
  } finally {
    await database.disconnect();
  }
}

// Run setup if called directly
if (require.main === module) {
  setupDatabase();
}

module.exports = { setupDatabase };