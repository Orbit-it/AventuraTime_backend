const Attendance = require("../models/Attendances");
const Notification = require("../models/Notification");
const pool = require('../config/db'); // Connexion PostgreSQL
const xlsx = require('xlsx');
const fs = require('fs');
const path = require('path');
const { response } = require("express");
const { ok } = require("assert");
const { processManualAttendance, updateAttendanceSummaryFromTimes,
   classifyAllPunchesWithLogs, processMonthlyAttendance } = require('../services/attendanceService');

const moment = require('moment');

moment.locale('fr');


exports.importAttendances = async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ success: false, message: 'Aucun fichier téléchargé' });
  }

  const filePath = req.file.path;

  try {
    const workbook = xlsx.readFile(filePath);
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const attendanceData = xlsx.utils.sheet_to_json(worksheet);

    let importedCount = 0;
    let skippedCount = 0;
    const errors = [];

    for (const [index, record] of attendanceData.entries()) {
      try {
        const rawDate = record.Date;

        const excelSerialNumber = parseFloat(rawDate);
        if (isNaN(excelSerialNumber)) {
          errors.push(`Ligne ${index + 2}: Format de date invalide`);
          skippedCount++;
          continue;
        }

        const jsDate = new Date(1899, 11, 30 + excelSerialNumber);
        const d = String(jsDate.getDate()).padStart(2, '0');
        const m = String(jsDate.getMonth() + 1).padStart(2, '0');
        const y = jsDate.getFullYear();
        const baseDate = new Date(`${y}-${m}-${d}`);

        if (isNaN(baseDate.getTime())) {
          errors.push(`Ligne ${index + 2}: Date invalide`);
          skippedCount++;
          continue;
        }

        const matricule = ('Matricule' in record) ? record.Matricule : record.sJobNo;

        if (!matricule || matricule == "1") {
          errors.push(`Ligne ${index + 2}: Matricule manquant`);
          skippedCount++;
          continue;
        }

        const employeeCheck = await pool.query(
          'SELECT id FROM employees WHERE attendance_id = $1',
          [matricule]
        );

        if (employeeCheck.rowCount === 0) {
          errors.push(`Ligne ${index + 2}: Employé avec matricule ${matricule} non trouvé`);
          skippedCount++;
          continue;
        }

        const employeeId = employeeCheck.rows[0].id;

        const punches = [];

        ['Time','Pointage_1', 'Pointage_2', 'Pointage_3', 'Pointage_4'].forEach(key => {
          if (key in record) {
            punches.push({ time: record[key] });
          }
        });


        for (const punch of punches) {
          if (punch.time == null || punch.time === '') continue;

          let timeString = '';

          // Cas 1: heure est un nombre Excel
          if (typeof punch.time === 'number') {
            const totalSeconds = Math.round(punch.time * 24 * 60 * 60);
            const hours = Math.floor(totalSeconds / 3600).toString().padStart(2, '0');
            const minutes = Math.floor((totalSeconds % 3600) / 60).toString().padStart(2, '0');
            const seconds = (totalSeconds % 60).toString().padStart(2, '0');
            timeString = `${hours}:${minutes}:${seconds}`;
          }
          // Cas 2: chaîne classique
          else if (typeof punch.time === 'string') {
            timeString = punch.time.trim();
          } else {
            errors.push(`Ligne ${index + 2}: Format de pointage inconnu (${punch.time})`);
            skippedCount++;
            continue;
          }

          const [hours, minutes, seconds] = timeString.split(':');

          if (isNaN(hours) || isNaN(minutes) || isNaN(seconds)) {
            errors.push(`Ligne ${index + 2}: Heure de pointage invalide (${timeString})`);
            skippedCount++;
            continue;
          }

          const punchTime = new Date(baseDate);
          punchTime.setHours(parseInt(hours), parseInt(minutes), parseInt(seconds));

          const duplicateCheck = await pool.query(
            `SELECT id FROM attendance_records 
             WHERE employee_id = $1 
             AND punch_time BETWEEN $2::timestamp - INTERVAL '1 minute' AND $2::timestamp + INTERVAL '1 minute'
`,
            [matricule, punchTime]
          );

          if (duplicateCheck.rowCount > 0) {
            console.log(`🔁 Doublon ignoré (ligne ${index + 2}, heure: ${timeString})`);
            skippedCount++;
            continue;
          }

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
        console.error(`❌ Erreur à la ligne ${index + 2} :`, error.message);
        errors.push(`Ligne ${index + 2}: Erreur de traitement - ${error.message}`);
        skippedCount++;
      }
    }

    fs.unlinkSync(filePath);

    await classifyAllPunchesWithLogs(); // Calssificartion des pointages après importation

    await processMonthlyAttendance();  // Calcul et mis à jour des pointages

    return res.json({
      success: true,
      message: 'Import de pointages terminé avec succès !',
      imported: importedCount,
      skipped: skippedCount,
      errors: errors.length > 0 ? errors : undefined
    });

  } catch (error) {
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


// coorection manuelle à partir de la page Pointage
exports.addAttendance = async (req, res) => {
  try {
    const { employee, date, getin, getout, autorizgetOut, autorizgetIn } = req.body;

    await updateAttendanceSummaryFromTimes(employee, date, getin, getout, autorizgetOut, autorizgetIn);
      res.status(200).json({
      ok: true,
      message: "Pointage corrigé avec succès !",
      corrected: true
    });
  } catch (error) {
    res.status(400).json({ error: error.message,
      message: `Erreur de correction de pointage: ${error}`,
      ok: false,
      corrected: false });
  }
};





// Supprimer un pointage
exports.deletePointage = async (req, res) => {
  try {
    const { id } = req.params; // On récupère l'ID du pointage à supprimer depuis les paramètres de la route

    // Vérification que l'ID existe
    if (!id) {
      return res.status(400).json({ error: "L'ID du pointage est requis" });
    }

    // Recherche et suppression du pointage
    const deletedAttendance = await Attendance.destroy({
      where: { 
        id: id,
        punch_source: 'MANUAL' // Optionnel: on ne supprime que les pointages manuels
      }
    });

    // Vérification si un pointage a bien été supprimé
    if (deletedAttendance === 0) {
      return res.status(404).json({ error: "Pointage non trouvé ou déjà supprimé" });
    }

    res.status(200).json({ 
      message: "Pointage supprimé avec succès",
      deletedId: id
    });

  } catch (error) {
    console.error("Erreur lors de la suppression du pointage:", error);
    res.status(500).json({ 
      error: "Erreur lors de la suppression du pointage",
      details: error.message 
    });
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

/* Fonction pour récupérer les données hebdomadaires
exports.getWeeklyAttendance = async (req, res) => {
  const { start_date, end_date, employee_id } = req.query;
  
  try {
      let query = `
          SELECT wa.*, e.name, e.attendance_id 
          FROM week_attendance wa
          JOIN employees e ON wa.employee_id = e.attendance_id
          WHERE 1=1
      `;
      const params = [];
      
      if (start_date && end_date) {
          query += ` AND wa.start_date >= $${params.length + 1} AND wa.end_date <= $${params.length + 2}`;
          params.push(start_date, end_date);
      }
      
      if (employee_id) {
          query += ` AND wa.employee_id = $${params.length + 1}`;
          params.push(employee_id);
      }
      
      query += ` ORDER BY e.name, wa.start_date`;
      
      const { rows } = await pool.query(query, params);
      
      res.json({
          success: true,
          data: rows
      });
  } catch (error) {
      res.status(500).json({
          success: false,
          message: 'Erreur lors de la récupération des données hebdomadaires'
      });
  }
};   */

// Fonction pour récupérer les données hebdomadaires et quotidiennes
exports.getWeeklyAttendance = async (req, res) => {
  const { start_date, end_date, employee_id } = req.query;
  
  try {
      // Requête pour les données hebdomadaires
      let weeklyQuery = `
          SELECT 
              wa.*, 
              e.name, 
              e.attendance_id,
              e.payroll_id,
              json_agg(
                  CASE WHEN asum.date IS NOT NULL THEN
                      json_build_object(
                          'date', asum.date,
                          'hours_worked', asum.hours_worked,
                          'missed_hour', asum.missed_hour,
                          'penalisable', asum.penalisable,
                          'sup_hour', asum.sup_hour,
                          'jc_value', asum.jc_value,
                          'jcx_value', asum.jcx_value,
                          'worked_hours_on_holidays', asum.worked_hours_on_holidays,
                          'night_hours', asum.night_hours,
                          'sunday_hour', asum.sunday_hour
                      )
                  ELSE NULL END
              ) FILTER (WHERE asum.date IS NOT NULL) AS daily_data
          FROM week_attendance wa
          JOIN employees e ON wa.employee_id = e.attendance_id
          LEFT JOIN attendance_summary asum ON 
              asum.employee_id = e.attendance_id AND
              asum.date BETWEEN wa.start_date AND wa.end_date
          WHERE 1=1
      `;
      
      const params = [];
      
      if (start_date && end_date) {
          weeklyQuery += ` AND wa.start_date >= $${params.length + 1} AND wa.end_date <= $${params.length + 2}`;
          params.push(start_date, end_date);
      }
      
      if (employee_id) {
          weeklyQuery += ` AND wa.employee_id = $${params.length + 1}`;
          params.push(employee_id);
      }
      
      weeklyQuery += ` 
          GROUP BY wa.id, e.name, e.attendance_id, e.payroll_id
          ORDER BY e.name, wa.start_date
      `;
      
      const { rows } = await pool.query(weeklyQuery, params);
      
      // Structurer les données pour la réponse
      const result = rows.map(row => ({
          ...row,
          daily_data: row.daily_data || [] // Assure un tableau vide si pas de données quotidiennes
      }));
      
      res.json({ result });
  } catch (error) {
      console.error('Error fetching weekly attendance:', error);
      res.status(500).json({
          success: false,
          message: 'Erreur lors de la récupération des données',
          error: error.message
      });
  }
};

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

/*
 * Récupère les données de présence hebdomadaires pour l'API
 * @param {Object} params - Paramètres de requête
 * @param {number} [params.employee_id] - Filtre par ID employé (optionnel)
 * @param {string} [params.month] - Mois au format 'YYYY-MM' (optionnel, ex: '2025-06')
 * @param {number} [params.limit] - Limite de résultats (optionnel)
 * @param {number} [params.offset] - Offset pour la pagination (optionnel)
 * @returns {Promise<Array>} - Tableau d'objets formatés pour l'API
 */
exports.getWeekAttendanceData = async (req, res) => {
  const { start_date, end_date, employee_id, month, limit, offset } = req.query;

  try {
    // Construction de la requête de base simplifiée
    let query = `
      SELECT 
        id,
        name,
        employee_id,
        start_date,
        end_date,
        total_night_hours,
        total_worked_hours,
        total_penalisable,
        total_sup,
        total_missed_hours,
        total_sunday_hours,
        total_jf,
        total_jc,
        total_htjf,
        total_jcx
      FROM week_attendance
      WHERE 1=1
    `;

    const params = [];
    let paramCount = 1;

    // Filtres
    if (start_date && end_date) {
      query += ` AND start_date >= $${paramCount++} AND end_date <= $${paramCount++}`;
      params.push(start_date, end_date);
    }

    if (employee_id) {
      query += ` AND employee_id = $${paramCount++}`;
      params.push(employee_id);
    }

    if (month) {
      const monthStart = moment(month, 'YYYY-MM').startOf('month').format('YYYY-MM-DD');
      const monthEnd = moment(month, 'YYYY-MM').endOf('month').format('YYYY-MM-DD');
      query += ` AND start_date >= $${paramCount++} AND end_date <= $${paramCount++}`;
      params.push(monthStart, monthEnd);
    }

    // Tri
    query += ` ORDER BY start_date DESC`;

    // Pagination
    if (limit) {
      query += ` LIMIT $${paramCount++}`;
      params.push(limit);
    }

    if (offset) {
      query += ` OFFSET $${paramCount++}`;
      params.push(offset);
    }

    // Exécution de la requête
    const { rows } = await pool.query(query, params);

    // Formatage des résultats simplifié
    const formattedData = rows.map(row => ({
      id: row.id,
      name: row.name,
      employee_id: row.employee_id,
      start_date: moment(row.start_date).format('DD-MM-YYYY'), // Format JJ-MM-AAAA
      end_date: moment(row.end_date).format('DD-MM-YYYY'),     // Format JJ-MM-AAAA
      total_night_hours: row.total_night_hours,
      total_worked_hours: row.total_worked_hours,
      total_penalisable: row.total_penalisable,
      total_sup: row.total_sup,
      total_missed_hours: row.total_missed_hours,
      total_sunday_hours: row.total_sunday_hours,
      total_jf: row.total_jf,
      total_jc: row.total_jc,
      total_htjf: row.total_htjf,
      total_jcx: row.total_jcx
    }));

    // Calcul du total pour la pagination
    let totalCount = rows.length;
    if (limit || offset) {
      const countQuery = `
        SELECT COUNT(*) 
        FROM week_attendance
        WHERE 1=1
        ${start_date && end_date ? `AND start_date >= $1 AND end_date <= $2` : ''}
        ${employee_id ? `AND employee_id = $${start_date && end_date ? 3 : 1}` : ''}
        ${month ? `AND start_date >= $${start_date && end_date ? (employee_id ? 4 : 3) : 1} 
                  AND end_date <= $${start_date && end_date ? (employee_id ? 5 : 4) : 2}` : ''}
      `;

      const countParams = [];
      if (start_date && end_date) countParams.push(start_date, end_date);
      if (employee_id) countParams.push(employee_id);
      if (month) {
        const monthStart = moment(month, 'YYYY-MM').startOf('month').format('YYYY-MM-DD');
        const monthEnd = moment(month, 'YYYY-MM').endOf('month').format('YYYY-MM-DD');
        countParams.push(monthStart, monthEnd);
      }

      const countResult = await pool.query(countQuery, countParams);
      totalCount = parseInt(countResult.rows[0].count, 10);
    }

    res.json(formattedData);

  } catch (error) {
    console.error('Error in getWeekAttendanceData:', error);
    res.status(500).json({
      success: false,
      message: 'Erreur lors de la récupération des données',
      error: error.message
    });
  }
};

exports.getMonthAttendanceData = async (req, res) => {
  const { start_date, end_date, employee_id } = req.query;

  try {
    // Construction de la requête de base simplifiée
    let query = `
      SELECT 
        payroll_id,
        employee_name,
        employee_id,
        month_start,
        month_end,
        total_night_hours,
        total_worked_hours,
        total_penalisable,
        total_sup,
        total_missed_hours,
        total_sunday_hours,
        total_jf,
        total_jc,
        total_htjf,
        total_jcx,
        periode_paie,
        prime_assiduite
      FROM monthly_attendance
      WHERE 1=1
    `;

    const params = [];
    let paramCount = 1;

    // Filtres
    if (start_date && end_date) {
      query += ` AND month_start >= $${paramCount++} AND month_end <= $${paramCount++}`;
      params.push(start_date, end_date);
    }

    if (employee_id) {
      query += ` AND employee_id = $${paramCount++}`;
      params.push(employee_id);
    }

    // Tri
    query += ` ORDER BY month_start DESC`;


    // Exécution de la requête
    const { rows } = await pool.query(query, params);

    // Formatage des résultats simplifié
    const formattedData = rows.map(row => ({
      payroll_id: row.payroll_id,
      employee_name: row.employee_name,
      employee_id: row.employee_id,
      month_start: moment(row.month_start).format('DD-MM-YYYY'), 
      month_end: moment(row.month_end).format('DD-MM-YYYY'),     
      total_night_hours: row.total_night_hours,
      total_worked_hours: row.total_worked_hours,
      total_penalisable: row.total_penalisable,
      total_sup: row.total_sup,
      total_missed_hours: row.total_missed_hours,
      total_sunday_hours: row.total_sunday_hours,
      total_jf: row.total_jf,
      total_jc: row.total_jc,
      total_htjf: row.total_htjf,
      total_jcx: row.total_jcx,
      periode_paie: row.periode_paie,
      prime_assiduite: row.prime_assiduite
    }));

    // Calcul du total pour la pagination

    res.json(formattedData);

  } catch (error) {
    console.error('Error in getMonthAttendanceData:', error);
    res.status(500).json({
      success: false,
      message: 'Erreur lors de la récupération des données',
      error: error.message
    });
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
              normal_hours,
              missed_hour, 
              penalisable, 
              is_weekend,
              is_conge,
              islayoff,
              jf_value,
              jc_value,
              jcx_value,
              worked_hours_on_holidays,
              isholidays,
              islayoff,
              sup_hour, 
              has_night_shift,
              night_hours, 
              sunday_hour, 
              is_accident,
              is_maladie,
              is_today,
              is_anomalie,
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

// Récupérer les pointages avec les données 
exports.getWeekAttendanceSummary = async (req, res) => {
  try {
      const { employee_id } = req.query;

      // 1️⃣ Construire la requête SQL de base
      let query = `
          SELECT 
              *
          FROM week_attendance_summary
          WHERE 1 = 1
      `;

      const params = [];

      // 2️⃣ Ajouter un filtre par employee_id si fourni
      if (employee_id) {
          query += ` AND employee_id = $${params.length + 1}`;
          params.push(employee_id);
      }

      // 3️⃣ Trier les résultats par date décroissante et employee_id croissant
      query += ` ORDER BY start_date DESC, employee_id ASC`;

      // 4️⃣ Exécuter la requête
      const result = await pool.query(query, params);

      if (!result || !result.rows.length) {
          return res.status(404).json({ message: "Aucune donnée trouvée." });
      }

      // 5️⃣ Retourner les résultats formatés
      res.status(200).json(result.rows);
  } catch (error) {
      console.error('❌ Erreur lors de la récupération des données :', error);
      res.status(500).json({ error: 'Erreur serveur lors de la récupération des données.' });
  }
};

// Ajout de Pointage manuel
exports.addManualAttendance = async (req, res) => {
  try {
    const { employee_id, punch_time, punch_type } = req.body;

    // Extraire la date (sans l'heure) du punch_time
    const punchDate = new Date(punch_time);
    punchDate.setHours(0, 0, 0, 0); // Réinitialiser l'heure à minuit

    // 1. Vérifier les pointages proches dans le temps (±2 minutes)
    const checkQuery = `
      SELECT id, punch_type 
      FROM attendance_records 
      WHERE employee_id = $1 
      AND ABS(EXTRACT(EPOCH FROM (punch_time - $2::timestamp))) <= 120
      ORDER BY punch_time DESC
      LIMIT 1;
    `;
    
    const nearbyRecords = await pool.query(checkQuery, [employee_id, punch_time]);

    // 2. Logique de correction/détection d'anomalies
    if (nearbyRecords.rows.length > 0) {
      const existingRecord = nearbyRecords.rows[0];
      
      // Cas 1: Même type de pointage (IN/OUT) → probable doublon
      if (existingRecord.punch_type === punch_type) {
        // Option 2: Mettre à jour le pointage existant
        const updateQuery = `
          UPDATE attendance_records 
          SET punch_time = $1, punch_source = 'MANUAL_CORRECTED'
          WHERE id = $2
          RETURNING *;
        `;
        const updated = await pool.query(updateQuery, [punch_time, existingRecord.id]);
        
        // Appliquer verifyAndFixPunchSequence pour ce jour spécifique
        await verifyAndFixPunchSequenceForDay(employee_id, punchDate);
        
        return res.status(200).json({
          ok: true,
          message: "Pointage existant mis à jour",
          corrected: true,
          previous_time: existingRecord.punch_time,
          record: updated.rows[0]
        });
      }
      
      // Cas 2: Pointage IN alors que le dernier était IN (oubli de OUT)
      if (punch_type === 'IN' && existingRecord.punch_type === 'IN') {
        // Ajouter un OUT automatique 1 minute avant le nouveau IN
        const autoOutTime = new Date(punch_time);
        autoOutTime.setMinutes(autoOutTime.getMinutes() - 1);
        
        await pool.query(
          `INSERT INTO attendance_records 
          (employee_id, punch_time, punch_type, punch_source) 
          VALUES ($1, $2, 'OUT', 'AUTO_CORRECTED')`,
          [employee_id, autoOutTime]
        );
      }
    }

    // 3. Insérer le nouveau pointage si aucun conflit ou après corrections
    const insertQuery = `
      INSERT INTO attendance_records 
      (employee_id, punch_time, punch_type, punch_source)
      VALUES ($1, $2, $3, 'MANUAL')
      RETURNING *;
    `;
    
    const newRecord = await pool.query(insertQuery, [employee_id, punch_time, punch_type]);

    // 4. Appliquer verifyAndFixPunchSequence pour ce jour spécifique
    await verifyAndFixPunchSequenceForDay(employee_id, punchDate);
    await processManualAttendance(punchDate, employee_id);

    res.status(201).json({
      ok: true,
      message: "Pointage ajouté avec succès",
      record: newRecord.rows[0],
      corrected: false
    });

  } catch (error) {
    console.error("Erreur addManualAttendance:", error);
    res.status(500).json({ 
      ok: false,
      error: "Erreur serveur",
      details: error.message 
    });
  }
};

// Nouvelle fonction pour appliquer verifyAndFixPunchSequence à un jour spécifique
async function verifyAndFixPunchSequenceForDay(employee_id, date) {
  console.log(`[Correction] Application de verifyAndFixPunchSequence pour l'employé ${employee_id} le ${date.toISOString().split('T')[0]}`);
  
  try {
    // 1. Récupération des données pour ce jour spécifique
    const { rows: punches } = await pool.query(`
      SELECT ar.id, ar.employee_id, e.name as employee_name, 
             ar.punch_time, ar.punch_type, ar.punch_source
      FROM attendance_records ar
      LEFT JOIN employees e ON ar.employee_id = e.attendance_id
      WHERE ar.employee_id = $1 
      AND DATE(ar.punch_time) = DATE($2)
      AND ar.punch_type IS NOT NULL
      ORDER BY ar.punch_time`,
      [employee_id, date]
    );

    if (punches.length === 0) {
      console.log('→ Aucun pointage à vérifier pour ce jour');
      return;
    }

    // 2. Structuration des données pour le traitement
    const dayKey = date.toISOString().split('T')[0];
    const employeeData = {
      name: punches[0]?.employee_name || 'Nom inconnu',
      days: {
        [dayKey]: punches
      }
    };

    // 3. Variables pour stocker les corrections
    const allCorrections = [];
    const allNotifications = [];
    const allReviews = [];

    // 4. Traitement spécifique pour ce jour
    const dayPunches = employeeData.days[dayKey];
    console.log(`→ ${dayPunches.length} pointages à vérifier pour (${employee_id}) le ${dayKey}`);

    // Filtrage avec marge (6h-22h)
    const dayShiftPunches = dayPunches.filter(punch => {
      const punchTime = new Date(punch.punch_time);
      const totalMinutes = punchTime.getHours() * 60 + punchTime.getMinutes();
      return totalMinutes >= 360 && totalMinutes <= 1320; // 6h=360min, 22h=1320min
    });

    if (dayShiftPunches.length === 0) {
      console.log('→ Aucun pointage dans la plage 6h-22h');
      return;
    }

    console.log(`→ ${dayShiftPunches.length} pointages à vérifier (6h-22h)`);

    // Vérification pointages impairs
    const today = new Date().toISOString().split('T')[0];
    if (dayShiftPunches.length % 2 !== 0 && dayKey !== today) {
      allNotifications.push({
        employeeId: employee_id,
        type: 'POINTAGE_IMPAIR',
        message: `Nombre impair de pointages (${dayShiftPunches.length}) pour (${employee_id}) le ${dayKey}`
      });
      console.log(`❌ Nombre impair de pointages (${dayShiftPunches.length})`);
    }

    let expectedNextType = null;
    let dayIssues = 0;

    for (let i = 0; i < dayShiftPunches.length; i++) {
      const punch = dayShiftPunches[i];
      const punchTime = new Date(punch.punch_time);
      const timeStr = punchTime.toLocaleTimeString('fr-FR', {hour: '2-digit', minute:'2-digit'});
      
      try {
        console.log(`  #${i+1} ${timeStr} [${punch.punch_type}]`);

        // Détection OUT matinal suspect (avant 12h sans IN préalable)
        if (punch.punch_type === 'OUT' && punchTime.getHours() < 12) {
          const isFirstPunch = i === 0;
          const hasNoPreviousIN = i > 0 && dayShiftPunches[i-1].punch_type !== 'IN';
          
          if (isFirstPunch || hasNoPreviousIN) {
            const errorMsg = `OUT matinal suspect à ${timeStr} sans IN préalable`;
            
            // Correction automatique conditionnelle
            if (isFirstPunch && punch.punch_source === 'AUTO') {
              console.log('  → Correction automatique: conversion en IN');
              allCorrections.push({
                id: punch.id,
                newType: 'IN',
                newSource: 'AUTO_CORRECTED'
              });
              allNotifications.push({
                employeeId: employee_id,
                type: 'CORRECTION_AUTO',
                message: `OUT matinal converti en IN pour ${employeeData.name} (${employee_id}) le ${dayKey} à ${timeStr}`
              });
              expectedNextType = 'OUT';
              continue;
            } else {
              allReviews.push({
                id: punch.id,
                reason: errorMsg
              });
              allNotifications.push({
                employeeId: employee_id,
                type: 'POINTAGE_SUSPECT',
                message: `${errorMsg} pour ${employeeData.name} (${employee_id}) le ${dayKey}`
              });
              dayIssues++;
            }
          }
        }

        // Vérification séquence IN/OUT
        if (i === 0 && punch.punch_type !== 'IN') {
          console.log(`  ❌ Premier pointage devrait être IN (${punch.punch_type})`);
          allReviews.push({
            id: punch.id,
            reason: 'Premier pointage devrait être IN'
          });
          dayIssues++;
          expectedNextType = 'OUT';
          continue;
        }

        if (expectedNextType && punch.punch_type !== expectedNextType) {
          console.log(`  ❌ Séquence incorrecte: attendu ${expectedNextType}, trouvé ${punch.punch_type}`);
          
          // Correction automatique si inversion simple détectée
          if (i < dayShiftPunches.length - 1 && 
              dayShiftPunches[i+1].punch_type === expectedNextType &&
              punch.punch_source === 'AUTO') {
            console.log('  → Correction automatique: inversion détectée');
            allCorrections.push({
              id: punch.id,
              newType: expectedNextType,
              newSource: 'AUTO_CORRECTED'
            });
          } else {
            allReviews.push({
              id: punch.id,
              reason: `Séquence incorrecte: attendu ${expectedNextType} après ${dayShiftPunches[i-1].punch_type}`
            });
          }
          dayIssues++;
        }
        
        expectedNextType = punch.punch_type === 'IN' ? 'OUT' : 'IN';

        // Vérification intervalle temporel
        if (i > 0) {
          const prevPunch = dayShiftPunches[i-1];
          const prevTime = new Date(prevPunch.punch_time);
          const timeDiff = (punchTime - prevTime) / (1000 * 60); // minutes
          
          // Intervalle trop court (<2 min)
          if (timeDiff < 2) {
            console.log(`  ⚠ Intervalle très court: ${timeDiff.toFixed(1)} minutes`);
            allReviews.push({
              id: punch.id,
              reason: `Intervalle très court (${timeDiff.toFixed(1)} min)`
            });
            dayIssues++;
          }
          
          // Pause longue (>15h entre OUT et IN suivant)
          if (prevPunch.punch_type === 'OUT' && punch.punch_type === 'IN' && timeDiff > 60 * 15) {
            console.log(`  ⚠ Pause longue: ${(timeDiff/60).toFixed(1)} heures`);
            allNotifications.push({
              employeeId: employee_id,
              type: 'PAUSE_LONGUE',
              message: `Pause longue de ${(timeDiff/60).toFixed(1)}h pour ${employeeData.name} (${employee_id}) le ${dayKey}`
            });
          }
        }
        
      } catch (err) {
        console.error(`  ❌ Erreur traitement:`, err.message);
        allReviews.push({
          id: punch.id,
          reason: `Erreur traitement: ${err.message.slice(0, 100)}`
        });
        dayIssues++;
      }
    }
    
    if (dayIssues > 0) {
      console.log(`  → ${dayIssues} problèmes détectés`);
    } else {
      console.log('  ✓ Aucune incohérence détectée');
    }

    // 5. Exécution des mises à jour
    // Corrections automatiques
    if (allCorrections.length > 0) {
      await pool.query(`
        UPDATE attendance_records ar
        SET punch_type = c.newType,
            punch_source = c.newSource
        FROM (VALUES ${allCorrections.map((c, i) => 
          `($${i*3+1}, $${i*3+2}, $${i*3+3})`
        ).join(',')}) AS c(id, newType, newSource)
        WHERE ar.id = c.id::integer`,
        allCorrections.flatMap(c => [c.id, c.newType, c.newSource])
      );
      console.log(`✓ ${allCorrections.length} corrections appliquées`);
    }
    
    // Marquages pour revue
    if (allReviews.length > 0) {
      await pool.query(`
        UPDATE attendance_records ar
        SET needs_review = TRUE,
            review_reason = r.reason
        FROM (VALUES ${allReviews.map((r, i) => 
          `($${i*2+1}, $${i*2+2})`
        ).join(',')}) AS r(id, reason)
        WHERE ar.id = r.id::integer`,
        allReviews.flatMap(r => [r.id, r.reason])
      );
      console.log(`✓ ${allReviews.length} pointages marqués pour revue`);
    }
    
    // Notifications RH
    if (allNotifications.length > 0) {
      await pool.query(`
        INSERT INTO hr_notifications 
        (employee_id, notification_type, message, created_at)
        VALUES ${allNotifications.map((_, i) => 
          `($${i*3+1}, $${i*3+2}, $${i*3+3}, NOW())`
        ).join(',')}`,
        allNotifications.flatMap(n => [n.employeeId, n.type, n.message])
      );
      console.log(`✓ ${allNotifications.length} notifications créées`);
    }

    console.log('[Fin] Correction terminée pour ce jour');
  } catch (error) {
    console.error('[ERREUR dans verifyAndFixPunchSequenceForDay]', error.stack);
    throw error;
  }
}


// Récupérer les stats depuis table monthly_attendance pour le tableau de bord
exports.getDashboardDatas = async (req, res) => {
  try {
    const { employee_id } = req.params;
    const { start_date, end_date } = req.query;

    // Par défaut : mois en cours
    const startDate = start_date || new Date(new Date().getFullYear(), new Date().getMonth(), 1);
    const endDate = end_date || new Date(new Date().getFullYear(), new Date().getMonth() + 1, 0);


    // Construction de la requête SQL
    let query = `
      SELECT
        SUM(total_nbr_abs) AS total_absence,
        SUM(total_conge) AS total_conges,
        SUM(total_accident) AS total_accident
      FROM monthly_attendance s
      WHERE s.month_start BETWEEN $1 AND $2
    `;

    const params = [startDate, endDate];

    if (employee_id) {
      query += ` AND s.employee_id = $3`;
      params.push(employee_id);
    }

    // Exécution de la requête
    const { rows } = await pool.query(query, params);
    const result = rows[0];

    // Calcul des taux
    const total_absence = parseFloat(result.total_absence) || 0;
    const total_conges = parseFloat(result.total_conges) || 0;
    const total_accident = parseFloat(result.total_accident) || 0;

    const total_jours = 30; // ou calcul dynamique si besoin
    const jours_effectivement_absents = total_absence + total_conges + total_accident;

    const taux_absence = ((jours_effectivement_absents / total_jours) * 100).toFixed(2);
    const taux_presence = (100 - taux_absence).toFixed(2);

    const response = {
      taux_presence: parseFloat(taux_presence),
      taux_absence: parseFloat(taux_absence),
      total_accident,
      total_conges
    };

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

exports.getMissedbyInterval = async (req, res) => {
  try {
    const { start_date, end_date } = req.query;

    // Set default dates (current month)
    const startDate = start_date || new Date(new Date().getFullYear(), new Date().getMonth(), 1);
    const endDate = end_date || new Date(new Date().getFullYear(), new Date().getMonth() + 1, 0);

    // Query to get active employees
    const employeeQuery = `
      SELECT id, name, attendance_id
      FROM employees
      WHERE is_active = TRUE
    `;
    const employeeResult = await pool.query(employeeQuery);
    const activeEmployees = employeeResult.rows; // Changed from emps to rows
    const activeEmployeeCount = activeEmployees.length;

    if (activeEmployeeCount === 0) {
      return res.json({
        taux_presence: 0,
        taux_absence: 0,
        total_absence: 0,
        message: "No active employees found"
      });
    }

    // Query to get missed attendances
    const attendanceQuery = `
      SELECT
        employee_id,
        date
      FROM attendance_summary
      WHERE date BETWEEN $1 AND $2 
      AND getin_ref IS NOT NULL 
      AND hours_worked IS NULL 
      AND isholidays IS NOT TRUE
      AND islayoff IS NOT TRUE
      AND has_night_shift IS NOT TRUE
    `;
    const attendanceParams = [startDate, endDate];
    const attendanceResult = await pool.query(attendanceQuery, attendanceParams);
    const missedAttendances = attendanceResult.rows;
    const totalAbsence = missedAttendances.length;

    // Calculate rates
    const tauxAbsence = totalAbsence > 0 ? 
      ((totalAbsence / activeEmployeeCount) * 100).toFixed(2) : 0;
    const tauxPresence = (100 - tauxAbsence).toFixed(2);

    const response = {
      taux_presence: parseFloat(tauxPresence),
      taux_absence: parseFloat(tauxAbsence),
      total_absence: totalAbsence,
      active_employees: activeEmployeeCount
    };

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

exports.getTodayAbsences = async (req, res) => {
  try {

    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const localDate = `${year}-${month}-${day}`;


    // 1. Récupérer tous les employés actifs
    const employeeQuery = `
      SELECT id, name, attendance_id, avatar
      FROM employees 
      WHERE is_active = TRUE
        AND department_id != 10
        AND attendance_id NOT IN (1, 284)
    `;
    const employeeResult = await pool.query(employeeQuery);
    const activeEmployees = employeeResult.rows;
    const activeEmployeeCount = activeEmployees.length;

    if (activeEmployeeCount === 0) {
      return res.json({
        success: true,
        message: "Aucun employé actif trouvé",
        data: {
          date: localDate,
          total_employees: 0,
          absences: []
        }
      });
    }

    // 2. Récupérer toutes les entrées d'attendance_summary pour aujourd'hui
    const attendanceQuery = `
      SELECT employee_id, status, getin, getout, is_anomalie
      FROM attendance_summary 
      WHERE date = $1
    `;
    const attendanceResult = await pool.query(attendanceQuery, [localDate]);
    const attendanceRecords = attendanceResult.rows;

    // 3. Créer un map des statuts par employee_id
    const statusMap = {};
    attendanceRecords.forEach(record => {
      statusMap[record.employee_id] = record.status;
    });

    // 4. Identifier les employés présents (avec pointage valide)
    const presentEmployeeIds = attendanceRecords
      .filter(record => record.getin != null || record.getout !=  null)
      .map(record => record.employee_id);

    // 5. Identifier les absents
    const absentEmployees = activeEmployees.filter(emp => 
      !presentEmployeeIds.includes(emp.attendance_id)
    );

    // 6. Préparer les données des absences avec le statut
    const absencesWithStatus = absentEmployees.map(emp => {
      const status = statusMap[emp.attendance_id] || 'not_checked';
      return {
        ...emp,
        status: status,
        absence_type: status === 'not_checked' ? 'no_check' : 'checked_but_no_work',
        absence_label: status === 'not_checked' 
          ? 'Aucun pointage' 
          : `Pointé - Statut: ${status}`
      };
    });

    // 7. Calculer les statistiques
    const totalAbsence = absencesWithStatus.length;
    const tauxAbsence = (totalAbsence / activeEmployeeCount * 100).toFixed(2);
    const tauxPresence = (100 - tauxAbsence).toFixed(2);

    // 8. Préparer la réponse
    const response = {
      date: localDate,
      total_employees: activeEmployeeCount,
      present_employees: activeEmployeeCount - totalAbsence,
      absent_employees: totalAbsence,
      absence_rate: parseFloat(tauxAbsence),
      presence_rate: parseFloat(tauxPresence),
      absences: absencesWithStatus.map(emp => ({
        employee_id: emp.attendance_id,
        name: emp.name,
        avatar: emp.avatar,
        status: emp.status,
        absence_type: emp.absence_type,
        absence_label: emp.absence_label
      })),
      summary: {
        by_status: absencesWithStatus.reduce((acc, emp) => {
          acc[emp.status] = (acc[emp.status] || 0) + 1;
          return acc;
        }, {}),
        by_type: {
          no_check: absencesWithStatus.filter(e => e.absence_type === 'no_check').length,
          checked_but_no_work: absencesWithStatus.filter(e => e.absence_type === 'checked_but_no_work').length
        }
      }
    };

    res.json({
      success: true,
      data: response
    });

  } catch (error) {
    console.error('Erreur:', error);
    res.status(500).json({
      success: false,
      error: "Erreur serveur",
      details: error.message
    });
  }
};



// Récupérer les notifications
exports.getNotifications = async (req, res) => {
  try {
    const { employee_id } = req.params; 

    // Construction de la requête SQL
    let query = `
      SELECT
        *
      FROM hr_notifications s
      
    `;


    if (employee_id) {
      query += ` AND s.employee_id = $3`;
    }

    // Exécution de la requête
    const { rows } = await pool.query(query);
    const result = rows;

    res.json(result);
  } catch (error) {
    console.error('Error fetching notification:', error);
    res.status(500).json({ 
      error: 'Erreur lors de la récupération des notifications',
      details: error.message,
      request_details: {
        query: req.query
      }
    });
  }
};


// Notifications lues
exports.markNotificationAsRead = async (req, res) => {
  const client = await pool.connect();
  
  try {
    const { notification_ids } = req.body;
    
    // Vérifier si on a des IDs
    if (!notification_ids || !Array.isArray(notification_ids) || notification_ids.length === 0) {
      return res.status(400).json({ error: "Liste d'IDs de notification invalide" });
    }

    await client.query('BEGIN'); // Début de transaction

    // Créer une liste de paramètres pour la requête ($1, $2, etc.)
    const params = notification_ids.map((_, index) => `$${index + 1}`).join(',');
    
    // Première requête: vérifier l'existence des notifications
    const checkQuery = {
      text: `SELECT COUNT(*) FROM hr_notifications WHERE id IN (${params}) AND is_read = false`,
      values: notification_ids
    };
    
    const checkResult = await client.query(checkQuery);
    const existingCount = parseInt(checkResult.rows[0].count);
    
    if (existingCount !== notification_ids.length) {
      await client.query('ROLLBACK');
      return res.status(404).json({ 
        error: "Certaines notifications n'existent pas ou sont déjà lues",
        details: {
          requested: notification_ids.length,
          found: existingCount
        }
      });
    }

    // Mettre à jour les notifications en une seule requête
    const updateQuery = {
      text: `UPDATE hr_notifications SET is_read = true WHERE id IN (${params})`,
      values: notification_ids
    };
    
    await client.query(updateQuery);
    await client.query('COMMIT'); // Validation de la transaction
    
    res.status(200).json({ 
      success: true,
      updated_count: existingCount
    });
    
  } catch (error) {
    await client.query('ROLLBACK'); // Annulation en cas d'erreur
    console.error("Erreur de mise à jour de notification:", error);
    res.status(500).json({ 
      error: "Erreur serveur",
      details: error.message 
    });
  } finally {
    client.release(); // Libération du client
  }
};









