const express = require("express");
const { addDepartment, getDepartments, deleteDepartment, updateDepartment } = require("../controllers/departmentController");

const router = express.Router();

router.post("/departments", addDepartment); // Ajouter un département
router.get("/departments", getDepartments); // Liste des départements
router.delete("/departments/:id", deleteDepartment); // Supprimer un département
router.put("/departments/:id", updateDepartment); // Mettre à jour un département

module.exports = router;
