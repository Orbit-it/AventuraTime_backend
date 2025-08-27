const express = require("express");
const { addEmployee, loginEmployee, getEmployees, getActiveEmployees, updateEmployee, getEmployeesByDepartment } = require("../controllers/employeeController");

const router = express.Router();



router.post("/employees", addEmployee); // Ajouter un employé
router.get("/employees", getEmployees); // Liste des employés
router.get("/active-employees", getActiveEmployees); // Liste des employés actifs
router.put("/employees/:id", updateEmployee); // Mettre à jour un employé
router.get("/employees/by-department/:id", getEmployeesByDepartment); // Liste des employés par département
router.post("/login", loginEmployee); // Login des employés


module.exports = router;