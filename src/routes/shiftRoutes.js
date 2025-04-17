const express = require('express');
const router = express.Router();
const workShiftController = require('../controllers/shiftController');

router.get('/shifts', workShiftController.getShifts);
router.post('/shifts', workShiftController.createShift);
router.put('/shifts/:id', workShiftController.updateShift);
router.delete('/shifts/:id', workShiftController.deleteShift);

module.exports = router;
