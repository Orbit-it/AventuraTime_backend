const Layoff = require('../models/Layoff');
const { apresAjoutIndisponibility } = require('../services/attendanceService');


// créer une nouvelle mise à pied
exports.addLayoff = async (req, res) => {
  try {
    const { employee_id, type, start_date, end_date, nb_jour } = req.body;
    const newLayoff = await Layoff.create({ employee_id, type, start_date, end_date, nb_jour });
    await apresAjoutIndisponibility(start_date, end_date, employee_id);
   
    res.status(201).json(newLayoff);
  } catch (error) {
    res.status(400).json({ error: error.message });
  }
};

// obtenir toutes les mises à pied
exports.getLayoffs = async (req, res) => {
  try {
    const layoffs = await Layoff.findAll();
    res.json(layoffs);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

// obtenir les mises à pied par employé
exports.getLayoffsByEmployee = async (req, res) => {
  try {
    const layoffs = await Layoff.findAll({ where: { employee_id: req.params.id } });
    res.json(layoffs);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

// mettre à jour une mise à pied
exports.updateLayoff = async (req, res) => {
  try {
    const layoff = await Layoff.findByPk(req.params.id);
    if (!layoff) {
      return res.status(404).json({ error: "Layoff not found" });
    }
    await layoff.update(req.body);
    res.json(layoff);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

// supprimer une mise à pied
exports.deleteLayoff = async (req, res) => {
  try {
    const layoff = await Layoff.findByPk(req.params.id);
    if (!layoff) {
      return res.status(404).json({ error: "Layoff not found" });
    }
    await layoff.destroy();
    res.json({ message: "Layoff deleted successfully" });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};