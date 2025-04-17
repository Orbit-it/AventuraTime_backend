const express = require("express");
const { getAttendanceStats, addAttendance,
   updateAttendance, getAttendanceWithMultipleShifts, addManualAttendance,
    getAttendanceSummary, importAttendances } =  require("../controllers/attendanceController");
const multer = require('multer');

const router = express.Router();

// Configuration de multer pour le stockage temporaire
const upload = multer({
    dest: 'uploads/',
    fileFilter: (req, file, cb) => {
      if (file.mimetype.includes('excel') || file.mimetype.includes('spreadsheet') || 
          ['.xlsx', '.xls'].includes(path.extname(file.originalname).toLowerCase())) {
        cb(null, true);
      } else {
        cb(new Error('Seuls les fichiers Excel sont autorisés'), false);
      }
    },
    limits: {
      fileSize: 5 * 1024 * 1024 // Limite à 5MB
    }
  });

router.post("/attendances", addAttendance); // Ajouter un pointage
router.get("/attendances", getAttendanceWithMultipleShifts); // Liste des pointages
router.put("/attendances/:id", updateAttendance); // Mettre à jour un pointage
router.get("/summary", getAttendanceSummary); // Recupérer les pointages calculés
router.post('/import-excel', upload.single('file'), importAttendances);
router.get('/attendance-stats/:start_date/:end_date/:employee_id?', getAttendanceStats);
router.post('/manual-attendance', addManualAttendance); // Ajouter un pointage manuel


module.exports = router;






