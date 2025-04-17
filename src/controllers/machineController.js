const Machine = require('../models/Machines');
const machineService = require('../services/attendanceService');

// ajouter une machine
exports.addMachine = async (req, res) => {
  try {
    const { ip_address, port, device_name, location } = req.body;
    const newMachine = await Machine.create({ ip_address, port, device_name, location });
    res.status(201).json(newMachine);
  } catch (error) {
    res.status(400).json({ error: error.message });
  }
};

// Récupérer toutes les machines
exports.getMachines = async (req, res) => {
  try {
    const machines = await Machine.findAll();
    res.json(machines);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

// Supprimer une machine
exports.deleteMachine = async (req, res) => {
  try {
    const machine = await Machine.findByPk(req.params.id);
    if (!machine) {
      return res.status(404).json({ error: "Machine not found" });
    }
    await machine.destroy();
    res.json({ message: "Machine deleted" });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

// Mettre à jour une machine
exports.updateMachine = async (req, res) => {
  try {
    const machine = await Machine.findByPk(req.params.id);
    if (!machine) {
      return res.status(404).json({ error: "Machine not found" });
    }
    await machine.update(req.body);
    res.json(machine);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

// Télécharger les données de présence
exports.downloadAttendance = async (req, res) => {
  try {
    const machine = await Machine.findByPk(req.params.id);
    if (!machine) {
      return res.status(404).json({ error: "Machine not found" });
    }
    const data = await machineService.downloadAttendance(machine);
    res.json(data);

    await machineService.updateAttendanceSummary(); // Mettre à jour après chak Téléchargement de pointage

  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};
