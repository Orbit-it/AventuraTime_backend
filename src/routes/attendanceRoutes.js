const express = require("express");
const Notification = require("../models/Notification");
const { getAttendanceStats, addAttendance, getWeekAttendanceSummary, getDashboardDatas, getNotifications,
   updateAttendance, getAttendanceWithMultipleShifts, addManualAttendance, markNotificationAsRead, deletePointage,
    getWeekAttendanceData, getMissedbyInterval, getTodayAbsences, getMonthAttendanceData, getAttendanceByPeriod, markNotificationAsResolved,
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
router.delete('/attendances/:id', deletePointage);  // supprimer un pointage
router.get("/summary", getAttendanceSummary); // Recupérer les pointages calculés
router.get('/summary/:start_date/:end_date/:employee_id', getAttendanceByPeriod);  // Route pour récupérer les pointages par période et employé
router.post('/import-excel', upload.single('file'), importAttendances);
router.get('/attendance-stats/:start_date/:end_date/:employee_id?', getAttendanceStats);
router.post('/manual-attendance', addManualAttendance); // Ajouter un pointage manuel
router.get('/weekly-attendance', getWeekAttendanceData); // Récupérer les pointages hebdomadaires
router.get('/month-attendance', getMonthAttendanceData); // Récupérer les pointages mensuel
router.get('/dashboard', getDashboardDatas);
router.get('/notif', getNotifications);
router.get('/absences', getTodayAbsences);  // recupérer les pointages en stats
router.post('/notifications/mark-as-read', markNotificationAsRead);
router.put('/notifications/:id/resolve', markNotificationAsResolved);




module.exports = router;






