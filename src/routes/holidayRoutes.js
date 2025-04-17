const express = require("express");
const { addHoliday, getHolidays, updateHoliday, deleteHoliday, getHolidayById } = require("../controllers/holidayController");

const router = express.Router();
router.post("/holidays", addHoliday); // Ajouter un jour férié
router.get("/holidays", getHolidays); // Liste des jours fériés
router.put("/holidays/:id", updateHoliday); // Mettre à jour un jour férié
router.delete("/holidays/:id", deleteHoliday); // Supprimer un jour férié
router.get("/holidays/:id", getHolidayById); // Obtenir un jour férié par ID


module.exports = router;
