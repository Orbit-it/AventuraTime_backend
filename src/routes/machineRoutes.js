const express = require('express');
const { addMachine, getMachines, deleteMachine, updateMachine, downloadAttendance } = require('../controllers/machineController');

const router = express.Router();

router.post('/machines', addMachine); // Ajouter une machine
router.get('/machines', getMachines); // Liste des machines
router.delete('/machines/:id', deleteMachine); // Supprimer une machine
router.put('/machines/:id', updateMachine); // Mettre à jour une machine
router.post('/machines/:id/attendance', downloadAttendance); // Télécharger les données de présence

module.exports = router;