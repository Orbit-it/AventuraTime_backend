const Holiday = require('../models/Holydays');

// créer un nouveau jour férié
exports.addHoliday = async (req, res) => {
  try {
    const { description, holiday_date, previous_working_day, next_working_day } = req.body;
    const newHoliday = await Holiday.create({ description, holiday_date, previous_working_day, next_working_day });
    res.status(201).json(newHoliday);
  } catch (error) {
    res.status(400).json({ error: error.message });
  }
};

// obtenir tous les jours fériés
exports.getHolidays = async (req, res) => {
  try {
    const holidays = await Holiday.findAll();
    res.json(holidays);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

// obtenir un jour férié par id
exports.getHolidayById = async (req, res) => {
  try {
    const holiday = await Holiday.findByPk(req.params.id);
    if (!holiday) {
      return res.status(404).json({ error: "Holiday not found" });
    }
    res.json(holiday);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

// mettre à jour un jour férié
exports.updateHoliday = async (req, res) => {
  try {
    const holiday = await Holiday.findByPk(req.params.id);
    if (!holiday) {
      return res.status(404).json({ error: "Holiday not found" });
    }
    await holiday.update(req.body);
    res.json(holiday);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

// supprimer un jour férié
exports.deleteHoliday = async (req, res) => {
  try {
    const holiday = await Holiday.findByPk(req.params.id);
    if (!holiday) {
      return res.status(404).json({ error: "Holiday not found" });
    }
    await holiday.destroy();
    res.json({ message: "Holiday deleted successfully" });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};