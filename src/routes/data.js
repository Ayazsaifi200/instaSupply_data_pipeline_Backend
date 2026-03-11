const express = require('express');
const fetchController = require('../controllers/fetchController');

const router = express.Router();

// GET /api/data - Get all records with caching
router.get('/', fetchController.getAllRecords.bind(fetchController));

// GET /api/data/stats - Get record statistics
router.get('/stats', fetchController.getRecordStats.bind(fetchController));

// GET /api/data/cache/status - Get cache status
router.get('/cache/status', fetchController.getCacheStatus.bind(fetchController));

// DELETE /api/data/cache - Clear cache
router.delete('/cache', fetchController.clearCache.bind(fetchController));

// GET /api/data/:id - Get record by ID
router.get('/:id', fetchController.getRecordById.bind(fetchController));

module.exports = router;