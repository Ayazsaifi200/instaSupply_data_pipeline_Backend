const express = require('express');
const uploadController = require('../controllers/uploadController');

const router = express.Router();

// POST /api/upload - Upload CSV file
router.post('/', uploadController.uploadCSV.bind(uploadController));

// GET /api/upload/history - Get upload history
router.get('/history', uploadController.getUploadHistory.bind(uploadController));

// GET /api/upload/:uploadId - Get specific upload status
router.get('/:uploadId', uploadController.getUploadStatus.bind(uploadController));

module.exports = router;