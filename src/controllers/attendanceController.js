const Attendance = require("../models/Attendances");
const pool = require('../config/db'); // Connexion PostgreSQL
const xlsx = require('xlsx');
const fs = require('fs');
const path = require('path');
const { response } = require("express");
const { ok } = require("assert");

exports.importAttendances = async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ success: false, message: 'Aucun fichier téléchargé' });
  }

  const filePath = req.file.path;

  try {
    // Lire le fichier Excel
    const workbook = xlsx.readFile(filePath);
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const attendanceData = xlsx.utils.sheet_to_json(worksheet);

    // Date de début pour filtrer (1er Mars 2025)
    const startDate = new Date('2025-03-01T00:00:00Z');
    let importedCount = 0;
    let skippedCount = 0;
    const errors = [];

    // Traitement des données
    for (const [index, record] of attendanceData.entries()) {
      try {
        // Extraire la date (format: "LUN24/03/2025")
        const dayDateMatch = record.Date?.match(/([A-Z]{3})(\d{2}\/\d{2}\/\d{4})/);
        if (!dayDateMatch) {
          errors.push(`Ligne ${index + 2}: Format de date invalide`);
          skippedCount++;
          continue;
        }

        const dateStr = dayDateMatch[2]; // "24/03/2025"
        const [day, month, year] = dateStr.split('/');
        const baseDate = new Date(`${year}-${month}-${day}`);

        if (isNaN(baseDate.getTime())) {
          errors.push(`Ligne ${index + 2}: Date invalide`);
          skippedCount++;
          continue;
        }

        const matricule = record.Matricule;
        if (!matricule) {
          errors.push(`Ligne ${index + 2}: Matricule manquant`);
          skippedCount++;
          continue;
        }

        // Vérifier si l'employé existe
        const employeeCheck = await pool.query(
          'SELECT id FROM employees WHERE attendance_id = $1',
          [matricule]
        ); 

        if (employeeCheck.rowCount === 0) {
          errors.push(`Ligne ${index + 2}: Employé avec matricule ${matricule} non trouvé`);
          skippedCount++;
          continue;
        }

        // Traiter chaque pointage
        const punches = [
          { time: record.Pointage_1 },
          { time: record.Pointage_2 },
          { time: record.Pointage_3 },
          { time: record.Pointage_4 }
        ];

        for (const punch of punches) {
          if (!punch.time || punch.time.trim() === '') continue;

          // Créer l'objet Date complet
          const [hours, minutes, seconds] = punch.time.split(':');
          const punchTime = new Date(baseDate);
          punchTime.setHours(parseInt(hours), parseInt(minutes), parseInt(seconds));

          /* Vérifier la date au delà du premier Mars 2025
          if (punchTime < startDate) {
            skippedCount++;
            continue;
          }  */

           // Vérifier les doublons
          const duplicateCheck = await pool.query(
            `SELECT id FROM attendance_records 
             WHERE employee_id = $1 
             AND punch_time BETWEEN $2 - INTERVAL '1 minute' AND $2 + INTERVAL '1 minute'`,
            [matricule, punchTime]
          );

          if (duplicateCheck.rowCount > 0) {
            skippedCount++;
            continue;
          }  

          // Insérer dans la base
          await pool.query(
            `INSERT INTO attendance_records 
             (employee_id, shift_id, punch_time, punch_source)
             VALUES ($1, NULL, $2, 'AUTO')
             ON CONFLICT (employee_id, punch_time, device_id) DO NOTHING`,
            [matricule, punchTime]
          );

          importedCount++;
        }
      } catch (error) {
        errors.push(`Ligne ${index + 2}: Erreur de traitement - ${error.message}`);
        skippedCount++;
      }
    }

    // Supprimer le fichier temporaire
    fs.unlinkSync(filePath);

    return res.json({
      success: true,
      message: 'Import de pointages terminé avec succès !',
      imported: importedCount,
      skipped: skippedCount,
      errors: errors.length > 0 ? errors : undefined
    });

  } catch (error) {
    // Nettoyer le fichier en cas d'erreur
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }
    
    console.error('Erreur lors de l\'import Excel:', error);
    return res.status(500).json({ 
      success: false, 
      message: 'Erreur lors du traitement du fichier Excel',
      error: error.message 
    });
  }
};

// Ajouter un pointage manuelle
exports.addAttendance = async (req, res) => {
  try {
    const { employee_id, punch_time } = req.body;
    const manualAttendance = await Attendance.create({ 
        employee_id,
        punch_time, 
        punch_type :'MANUAL'
     });
    res.status(201).json(manualAttendance);
  } catch (error) {
    res.status(400).json({ error: error.message });
  }
};

// Récupérer tous les pointages
exports.getAttendance = async (req, res) => {
  try {
    const attendances = await Attendance.findAll();
    res.json(attendances);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

// Récupérer les stats depuis table attendance_summary avec filtrage par période
exports.getAttendanceStats = async (req, res) => {
  try {
    const { start_date, end_date, employee_id } = req.params;

    // Validation des paramètres
    if (!start_date || !end_date) {
      return res.status(400).json({ 
        error: 'Les dates de début et de fin sont requises',
        received_params: req.params
      });
    }

    const startDate = new Date(start_date);
    const endDate = new Date(end_date);

    if (isNaN(startDate.getTime()) || isNaN(endDate.getTime())) {
      return res.status(400).json({ 
        error: 'Format de date invalide. Utilisez YYYY-MM-DD',
        example: '/attendance-stats/2023-01-01/2023-01-31/123'
      });
    }

    if (startDate > endDate) {
      return res.status(400).json({ 
        error: 'La date de fin doit être postérieure à la date de début',
        received: {
          start_date,
          end_date
        }
      });
    }

    // Debug: Afficher les paramètres reçus
    console.log('Received params:', { start_date, end_date, employee_id });

    // Construction de la requête SQL de base
    let query = `
      SELECT
        s.employee_id,
        e.name AS employee_name,
        e.attendance_id,
        SUM(s.nbr_absence) AS total_absence,
        SUM(s.nbr_retard) AS total_retard,
        SUM(s.nbr_depanti) AS total_depanti
      FROM attendance_summary s
      JOIN employees e ON s.employee_id = e.attendance_id
      WHERE s.date BETWEEN $1 AND $2
    `;

    const params = [start_date, end_date];

    // Ajout du filtre employee_id si fourni
    if (employee_id && employee_id !== 'undefined') {
      query += ` AND s.employee_id = $${params.length + 1}`;
      params.push(employee_id);
    }

    query += `
      GROUP BY s.employee_id, e.name, e.attendance_id
      ORDER BY e.name
    `;

    // Debug: Afficher la requête finale
    console.log('Final query:', query);
    console.log('Query params:', params);

    // Exécution de la requête
    const { rows } = await pool.query(query, params);

    // Formatage de la réponse
    const response = {
      metadata: {
        generated_at: new Date().toISOString(),
        period: {
          start_date,
          end_date,
          days: (endDate - startDate) / (1000 * 60 * 60 * 24) + 1
        },
        employee_filter: employee_id || 'all',
        record_count: rows.length
      },
      data: employee_id && rows.length === 1 ? rows[0] : rows
    };

    if (rows.length === 0) {
      response.message = employee_id 
        ? `Aucune donnée trouvée pour l'employé ${employee_id} sur cette période` 
        : 'Aucune donnée trouvée pour cette période';
      return res.status(404).json(response);
    }

    res.json(response);
  } catch (error) {
    console.error('Error fetching attendance stats:', error);
    res.status(500).json({ 
      error: 'Erreur serveur lors de la récupération des statistiques',
      details: error.message,
      request_details: {
        params: req.params,
        query: req.query
      }
    });
  }
};
/* Supprimer un pointage :::::: à voir
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
};   */

// Corriger un pointage 

exports.updateAttendance = async (req, res) => {
  try {
    const attendance = await Attendance.findByPk(req.params.id);
    if (!attendance) {
      return res.status(404).json({ error: "Attendance not found" });
    }
    await attendance.update(req.body);
    res.json(attendance);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};



// Récupérer les pointages avec le statut

exports.getAttendanceWithMultipleShifts = async (req, res) => {
  try {
    const { startDate, endDate } = req.query;

    const query = `
      SELECT 
          a.employee_id,
          a.punch_time,
          a.punch_type,
          a.punch_source,
          w.start_time,
          w.end_time,
          w.crosses_midnight,
          w.day_of_week,
          CASE 
              WHEN w.crosses_midnight = TRUE 
                  AND a.punch_time::TIME BETWEEN '00:00' AND w.end_time 
                  THEN 'Night Shift'
              WHEN a.punch_time::TIME BETWEEN w.start_time AND w.end_time THEN 'On Time'
              WHEN a.punch_time::TIME < w.start_time THEN 'Early'
              ELSE 'Late'
          END AS status
      FROM attendance_records a
      LEFT JOIN work_shifts w 
      ON a.employee_id = w.employee_id 
      WHERE a.punch_time::DATE BETWEEN $1 AND $2
      ORDER BY a.employee_id, a.punch_time;
    `;

    const { rows } = await pool.query(query, [startDate, endDate]);
    res.json(rows);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Server error' });
  }
};


// Récupérer les pointages avec les données 
exports.getAttendanceSummary = async (req, res) => {
  try {
      const { employee_id } = req.query;

      // 1️⃣ Construire la requête SQL de base
      let query = `
          SELECT 
              employee_id, 
              TO_CHAR(date, 'YYYY-MM-DD') AS date,  -- Formater la date en YYYY-MM-DD
              status, 
              hours_worked, 
              missed_hour, 
              penalisable, 
              is_weekend,
              is_conge,
              islayoff,
              worked_hours_on_holidays,
              isholidays,
              islayoff,
              sup_hour, 
              has_night_shift,
              night_hours, 
              sunday_hour, 
              is_accident,
              is_maladie,
              is_congex,
              night_getin,
              night_getout,
              autoriz_getin,
              autoriz_getout,
              getin,  -- Récupérer l'heure sous la forme hh:mm:ss
              getout  -- Récupérer l'heure sous la forme hh:mm:ss
          FROM attendance_summary
          WHERE 1 = 1
      `;

      const params = [];

      // 2️⃣ Ajouter un filtre par employee_id si fourni
      if (employee_id) {
          query += ` AND employee_id = $${params.length + 1}`;
          params.push(employee_id);
      }

      // 3️⃣ Trier les résultats par date décroissante et employee_id croissant
      query += ` ORDER BY date DESC, employee_id ASC`;

      // 4️⃣ Exécuter la requête
      const result = await pool.query(query, params);

      if (!result || !result.rows.length) {
          return res.status(404).json({ message: "Aucune donnée trouvée." });
      }

      // 5️⃣ Formater les heures en "hh:mm" (ignorer les secondes)
      const formattedResults = result.rows.map(row => {
          // Fonction pour extraire "hh:mm" d'une chaîne "hh:mm:ss"
          const formatTime = (time) => {
              if (!time) return null; // Si l'heure est null, retourner null
              return time.slice(0, 5); // Extraire les 5 premiers caractères (hh:mm)
          };

          return {
              ...row,
              getin: formatTime(row.getin),  // Formater getin en "hh:mm"
              getout: formatTime(row.getout), // Formater getout en "hh:mm"
              night_getin: formatTime(row.night_getin),
              night_getout: formatTime(row.night_getout),
              autoriz_getin: formatTime(row.autoriz_getin),
              autoriz_getout: formatTime(row.autoriz_getout)

          };
      });

      // 6️⃣ Retourner les résultats formatés
      res.status(200).json(formattedResults);
  } catch (error) {
      console.error('❌ Erreur lors de la récupération des données :', error);
      res.status(500).json({ error: 'Erreur serveur lors de la récupération des données.' });
  }
};

// Ajout de Pointage manuel
exports.addManualAttendance = async (req, res) => {
  try {
    const { employee_id, punch_time, punch_type } = req.body;

    // Vérifier si le pointage existe déjà
    const existingAttendance = await Attendance.findOne({
      where: {
        employee_id,
        punch_time
      }
    });

    if (existingAttendance) {
      return res.status(409).json({ error: "Ce pointage existe déjà:même Date et même Heure." });
    }

    // Insérer dans la base de données
    const query = `
    INSERT INTO attendance_records (employee_id, punch_time, punch_type, punch_source)
    VALUES ($1, $2, $3, 'MANUAL');
    `;

    await pool.query(query, [employee_id, punch_time, punch_type]);



    const response = {
      ok: true,
      message: "Pointage ajouté avec succès.",
    };

    console.log("Nouvelle entrée de pointage:");

    res.status(201).json(response);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};







