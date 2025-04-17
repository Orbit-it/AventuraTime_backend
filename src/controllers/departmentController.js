const Department = require("../models/Departments");

// Ajouter un département
exports.addDepartment = async (req, res) => {
  try {
    const { code, name, responsible_id } = req.body;
    const newDepartment = await Department.create({ code, name, responsible_id });
    res.status(201).json(newDepartment);
  } catch (error) {
    res.status(400).json({ error: error.message });
  }
};

// Récupérer tous les départements
exports.getDepartments = async (req, res) => {
  try {
    const departments = await Department.findAll();
    res.json(departments);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

// Supprimer un département
exports.deleteDepartment = async (req, res) => {
  try {
    const department = await Department.findByPk(req.params.id);
    if (!department) {
      return res.status(404).json({ error: "Department not found" });
    }
    await department.destroy();
    res.json({ message: "Department deleted" });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

// Mettre à jour un département
exports.updateDepartment = async (req, res) => {
  try {
    const department = await Department.findByPk(req.params.id);
    if (!department) {
      return res.status(404).json({ error: "Department not found" });
    }
    await department.update(req.body);
    res.json(department);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};
