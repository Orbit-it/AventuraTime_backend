const Employees = require('../models/Employees');

// Ajouter un employé plus avec un avatar
exports.addEmployee = async (req, res) => {
  try {
    // req.body contient déjà avatarPath si uploadé
    const newEmployee = await Employees.create(req.body);
    res.status(201).json(newEmployee);
  } catch (error) {
    res.status(400).json({ error: error.message });
  }
};

// Login des employés
exports.loginEmployee = async (req, res) => {
  try {
    const { username, password } = req.body;
    const employee = await Employees.findOne({ where: { username, password } });
    if (!employee) {
      return res.status(401).json({ error: "Invalid credentials" });
    }
    res.json(employee);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};


// Récupérer tous les employés
exports.getEmployees = async (req, res) => {
  try {
    const employees = await Employees.findAll({
      order: [['payroll_id', 'ASC']]
    });
    res.json(employees);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

// Récupérer tous les employés actifs
exports.getActiveEmployees = async (req, res) => {
  try {
    const activeEmployees = await Employees.findAll({
      where: {
        is_active: true
      },
      order: [['payroll_id', 'ASC']]
    });
    res.json(activeEmployees);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

// Récupérer les emloyés par departement
exports.getEmployeesByDepartment = async (req, res) => {
  try {
    const employees = await Employees.findAll({ where: { department_id: req.params.id } });
    res.json(employees);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

// Mettre à jour un employé
exports.updateEmployee = async (req, res) => {
  try {
    const employee = await Employees.findByPk(req.params.id);
    if (!employee) {
      return res.status(404).json({ error: "Employee not found" });
    }
    await employee.update(req.body);
    res.json(employee);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};