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

  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};


// Backend: Single endpoint implementation
exports.computeAttendance = async (req, res) => {
  try {
    const { machines, failedMachines } = req.body;

    // Configuration de l'en-tête une seule fois
    res.setHeader('Content-Type', 'application/json');
    
    // Fonction helper pour envoyer des mises à jour de progression
    const sendProgressUpdate = (progress, message) => {
      res.write(JSON.stringify({ progress, message }) + '\n');
    };

    // Étape 1: Classification des pointages
    sendProgressUpdate(40, "Classification des pointages...");
    await machineService.classifyAllPunchesWithLogs();

    // Étape 2: Traitement des présences mensuelles
    sendProgressUpdate(70, "Calcul des présences mensuelles...");
    await machineService.processMonthlyAttendance();

    // Réponse finale
    sendProgressUpdate(100, 
      `✅ Traitement terminé pour tous les employés\n` +
      `✅ Traitement des résumés d'attendance terminé\n` +
      `${failedMachines.length > 0 ? `⚠ ${failedMachines.length} machines en échec` : '✔ Tous les pointages traités'}`);

    // Fermer la réponse
    res.end();

  } catch (error) {
    if (!res.headersSent) {
      res.status(500).json({
        success: false,
        message: `Erreur de traitement: ${error.message}`
      });
    } else {
      console.error("Erreur après envoi des en-têtes:", error);
      // Envoyer un message d'erreur comme dernière chunk si possible
      res.write(JSON.stringify({ 
        error: true,
        message: `Erreur finale: ${error.message}`
      }) + '\n');
      res.end();
    }
  }
};

