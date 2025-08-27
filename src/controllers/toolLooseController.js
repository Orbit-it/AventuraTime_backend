const ToolLoose = require('../models/ToolLoose');

/* Créer une nouvelle perte d'outil     employee_id: number;
  tool_name: string;
  tool_price: number;
  loss_date: string;
  is_paid: boolean;
  notes: string;  */
exports.addToolLoose = async (req, res) => {
  try {
    const { employee_id, date, price, is_payed, tool, notes } = req.body;
    const newToolLoose = await ToolLoose.create({ 
      employee_id, 
      date, 
      price, 
      is_payed, 
      tool,
      notes
    });
    
    res.status(201).json(newToolLoose);
  } catch (error) {
    res.status(400).json({ error: error.message });
  }
};

// Obtenir toutes les pertes d'outils
exports.getToolLooses = async (req, res) => {
  try {
    const toolLooses = await ToolLoose.findAll({
      order: [['date', 'DESC']] // Tri par date décroissante
    });
    res.json(toolLooses);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

// Obtenir les pertes d'outils par employé
exports.getToolLoosesByEmployee = async (req, res) => {
  try {
    const toolLooses = await ToolLoose.findAll({ 
      where: { employee_id: req.params.id },
      order: [['date', 'DESC']]
    });
    res.json(toolLooses);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

// Obtenir les pertes d'outils non payées
exports.getUnpaidToolLooses = async (req, res) => {
  try {
    const toolLooses = await ToolLoose.findAll({ 
      where: { is_payed: false },
      order: [['date', 'DESC']]
    });
    res.json(toolLooses);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

// Mettre à jour une perte d'outil
exports.updateToolLoose = async (req, res) => {
  try {
    const toolLoose = await ToolLoose.findByPk(req.params.id);
    if (!toolLoose) {
      return res.status(404).json({ error: "Perte d'outil non trouvée" });
    }
    
    await toolLoose.update(req.body);
    res.json(toolLoose);

  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

// Supprimer une perte d'outil
exports.deleteToolLoose = async (req, res) => {
  try {
    const toolLoose = await ToolLoose.findByPk(req.params.id);
    if (!toolLoose) {
      return res.status(404).json({ error: "Perte d'outil non trouvée" });
    }
    
    await toolLoose.destroy();
    res.json({ message: "Perte d'outil supprimée avec succès" });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

// Marquer une perte comme payée
exports.markAsPaid = async (req, res) => {
  try {
    const toolLoose = await ToolLoose.findByPk(req.params.id);
    if (!toolLoose) {
      return res.status(404).json({ error: "Perte d'outil non trouvée" });
    }
    
    await toolLoose.update({ is_payed: true });
    res.json(toolLoose);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};