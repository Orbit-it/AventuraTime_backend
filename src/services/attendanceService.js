const Zkteco = require("zkteco-js");
const pool = require("../config/db");
const { TIME } = require("sequelize");
const xlsx = require('xlsx');
const fs = require('fs');
const e = require("express");
const moment = require('moment');

moment.locale('fr');


/**
 * Marque les pointages problématiques pour vérification humaine
 * param {number} employeeId - ID de l'employé concerné
 */
async function flagForManualReview(employeeId) {
    // 1. Marquer les enregistrements problématiques
    await pool.query(`
        UPDATE attendance_records
        SET needs_review = TRUE
        WHERE employee_id = $1 
        AND (
            (punch_type IS NULL)
            OR (id IN (
                SELECT current.id
                FROM attendance_records current
                JOIN attendance_records next ON (
                    next.employee_id = current.employee_id 
                    AND next.punch_time > current.punch_time
                    AND next.punch_time - current.punch_time < INTERVAL '15 minutes'
                )
                WHERE current.employee_id = $1
                AND current.punch_type = next.punch_type
            ))
        )`,
        [employeeId]
    );

    // 2. Notifier les RH - version corrigée
    const problematicCount = await pool.query(
        `SELECT COUNT(*)::integer as count FROM attendance_records 
         WHERE employee_id = $1 AND needs_review`,
        [employeeId]
    );

    if (problematicCount.rows[0].count > 0) {
        await pool.query(`
            INSERT INTO hr_notifications 
            (employee_id, notification_type, message)
            VALUES ($1, 'ATTENDANCE_REVIEW', $2)`,
            [employeeId, `${problematicCount.rows[0].count} pointages nécessitent une vérification`]
        );
    }
}

async function flagAsProcessingError(employeeId) {
    await pool.query(`
        INSERT INTO processing_errors 
        (employee_id, error_type, timestamp)
        VALUES ($1, 'AUTO_PUNCH_CLASSIFICATION', NOW())`,
        [employeeId]
    );
}

// Fonction pour classer automatiquement tous les pointages
async function classifyAllPunchesWithLogs() {
    console.log('[Début] Classification de tous les pointages non traités');
    
    try {
        // 1. Récupération des pointages
        console.time('Récupération des données');
        const punches = await pool.query(`
            SELECT id, employee_id, punch_time 
            FROM attendance_records 
            WHERE punch_type IS NULL
            ORDER BY employee_id, punch_time`
        );
        console.timeEnd('Récupération des données');
        console.log(`→ ${punches.rows.length} pointages à traiter`);

        if (punches.rows.length === 0) {
            console.log('[Fin] Aucun pointage à classifier');
            return;
        }

        // 2. Groupement par employé
        const employeesPunches = {};
        punches.rows.forEach(punch => {
            if (!employeesPunches[punch.employee_id]) {
                employeesPunches[punch.employee_id] = [];
            }
            employeesPunches[punch.employee_id].push(punch);
        });
        
        const employeeIds = Object.keys(employeesPunches);
        console.log(`→ ${employeeIds.length} employés concernés`);

        // 3. Traitement par employé
        for (const employeeId of employeeIds) {
            const employeePunches = employeesPunches[employeeId];
            console.log(`\n--- Traitement employé ${employeeId} (${employeePunches.length} pointages) ---`);
            
            let lastPunchType = null;
            let isNightShift = false;
            let currentShiftDay = null;

            for (let i = 0; i < employeePunches.length; i++) {
                const punch = employeePunches[i];
                const punchTime = new Date(punch.punch_time);
                const hours = punchTime.getHours();
                const minutes = punchTime.getMinutes();
                const timeStr = punchTime.toLocaleTimeString('fr-FR', {hour: '2-digit', minute:'2-digit'});
                const currentDay = punchTime.getDate();
                
                try {
                    console.log(`\nPointage #${i+1} à ${timeStr}`);
                    
                    // Détection des plages horaires de 5h50 à 6h10 / Nuit à partir de 21h
                    const isEarlyMorning = (hours === 5 && minutes > 50) || (hours === 6 && minutes <= 10);
                    const isNightStart = hours >= 21;
                    
                    // Premier pointage
                    if (i === 0) {
                        if (isEarlyMorning) {
                            // Pointage tôt le matin est probablement un OUT
                            punch.punch_type = 'OUT';
                            console.log('→ Premier pointage matinal traité comme OUT');
                        } else {
                            punch.punch_type = 'IN';
                            isNightShift = isNightStart;
                            currentShiftDay = isNightShift ? punchTime.getDate() : currentDay;
                            console.log(isNightShift ? '→ Début détecté: shift de nuit' : '→ Début détecté: shift de jour');
                        }
                    } 
                    // Pointages suivants
                    else {
                        const prevPunchTime = new Date(employeePunches[i-1].punch_time);
                        const prevHours = prevPunchTime.getHours();
                        const timeDiff = (punchTime - prevPunchTime) / (1000 * 60); // différence en minutes
                        
                        // Cas spécial: pointage entre 6h00 et 6h10 après un shift de nuit
                        if (isEarlyMorning ) {
                            punch.punch_type = 'OUT';
                            isNightShift = false;
                            console.log('→ Fin du shift de nuit (OUT automatique 6h-6h10)');
                        }
                        // Début potentiel d'un nouveau shift de nuit
                        else if (isNightStart && !isNightShift && timeDiff > 60) {
                            punch.punch_type = 'IN';
                            isNightShift = true;
                            currentShiftDay = punchTime.getDate();
                            console.log('→ Nouveau shift de nuit détecté');
                        }
                        // Fin de shift de nuit après 6h10
                        else if (isNightShift && (hours > 6 || (hours === 6 && minutes > 10))) {
                            punch.punch_type = 'OUT';
                            isNightShift = false;
                            console.log('→ Fin du shift de nuit après 6h10');
                        }
                        // Alternance normale IN/OUT
                        else {
                            punch.punch_type = lastPunchType === 'IN' ? 'OUT' : 'IN';
                            console.log(`→ Alternance normale (dernier type: ${lastPunchType})`);
                        }
                    }
                    
                    lastPunchType = punch.punch_type;

                    // Mise à jour en base
                    await pool.query(
                        `UPDATE attendance_records 
                        SET punch_type = $1, 
                            needs_review = false
                        WHERE id = $2`,
                        [punch.punch_type, punch.id]
                    );

                    console.log(`✓ Type: ${punch.punch_type} | Mis à jour en base`);
                    
                } catch (err) {
                    console.error(`❌ Erreur sur pointage ${punch.id}:`, err.message);
                    console.error('Détails erreur:', {
                        query: `UPDATE... WHERE id = ${punch.id}`,
                        params: [punch.punch_type, punch.id]
                    });

                    await pool.query(
                        `UPDATE attendance_records 
                        SET needs_review = true,
                            review_reason = $1
                        WHERE id = $2`,
                        [`Erreur traitement: ${err.message.slice(0, 100)}`, punch.id]
                    );
                }
            }
        }
        
        console.log('\n[Fin] Classification terminée');
    } catch (error) {
        console.error('[ERREUR GLOBALE]', error.stack);
        throw error;
    }
}

// Création de Attendance_summary OK
async function initAttendanceSummary(employeeId, date) {
    if (!Number.isInteger(Number(employeeId)) || isNaN(new Date(date).getTime())) {
        throw new Error(`Paramètres invalides: employeeId=${employeeId}, date=${date}`);
    }

    const client = await pool.connect();
    try {
        console.log(`🟢 Initialisation de l'attendance_summary pour employé ${employeeId} à la date ${date}`);

        // Vérifier si une entrée existe déjà
        const existingEntry = await client.query(
            `SELECT 1 FROM attendance_summary WHERE employee_id = $1 AND date = $2`,
            [employeeId, date]
        );

        if (existingEntry.rowCount === 0) {
            // Insérer une nouvelle ligne avec les valeurs par défaut
            await client.query(`
                INSERT INTO attendance_summary (
                    employee_id, date, is_weekend, is_saturday, is_sunday, created_at, updated_at
                ) VALUES (
                    $1, 
                    $2, 
                    EXTRACT(DOW FROM $2::DATE) IN (0, 6), 
                    EXTRACT(DOW FROM $2::DATE) = 6, 
                    EXTRACT(DOW FROM $2::DATE) = 0, 
                    NOW(), 
                    NOW()
                );
            `, [employeeId, date]);
            
            console.log(`✅ Nouvelle entrée ajoutée pour ${employeeId} à la date ${date}`);
        } else {
            console.log(`ℹ️ Une entrée existe déjà pour ${employeeId} à la date ${date}, aucune action nécessaire.`);
        }
    } catch (error) {
        console.error(`❌ Erreur lors de l'initialisation:`, error.message);
        throw error;
    } finally {
        client.release();
    }
}

// Gestion des jours fériés 
async function employeeHoliday(date, employeeId) {
    if (!Number.isInteger(Number(employeeId)) || isNaN(new Date(date).getTime())) {
        throw new Error(`Paramètres invalides: employeeId=${employeeId}, date=${date}`);
    }

    const client = await pool.connect();

    let is_holiday = false;
    let is_present_previous = false;
    let is_present_next = false;

    try {
        console.log(`📅 Check Jour férié pour l'employé ${employeeId} à la date ${date}`);

        // verifier si ce jour est un jour férié
        const holidayQuery = `
            SELECT holiday_date, previous_working_day, next_working_day 
            FROM public_holidays
            WHERE holiday_date = $1
        `;
        const holidayResult = await client.query(holidayQuery, [date]);

        if (holidayResult.rowCount > 0) {
            is_holiday = true;
            console.log(`🚫 Jour férié pour ${employeeId} à la date ${date}`);

            // verifier si le previous_working_day l'employé a travaillé
            const previousDate = holidayResult.rows[0].previous_working_day;
            const nextDate = holidayResult.rows[0].next_working_day;
            
            const previousWorkQuery = `
                SELECT 1 FROM attendance_summary
                WHERE employee_id = $1
                AND date = $2
                AND get_holiday = TRUE 
            `;
            const previousWorkResult = await client.query(previousWorkQuery, [employeeId, previousDate]);

            if (previousWorkResult.rowCount > 0) {
                console.log(`✅ L'employé ${employeeId} a travaillé le jour précédent (${previousDate})`);
                is_present_previous = true;
            }
            
            const nextWorkQuery = ` 
                SELECT 1 FROM attendance_summary
                WHERE employee_id = $1
                AND date = $2
                AND get_holiday = TRUE
            `;
            const nextWorkResult = await client.query(nextWorkQuery, [employeeId, nextDate]);
            
            if (nextWorkResult.rowCount > 0) {
                console.log(`✅ L'employé ${employeeId} a travaillé le jour suivant (${nextDate})`);
                is_present_next = true;
            }

            if (is_present_previous && is_present_next) {
                await client.query(`
                    UPDATE attendance_summary
                    SET 
                        penalisable = 0,
                        missed_hour = 0,
                        status = 'jf_win',
                        isholidays = TRUE,
                        get_holiday = TRUE,
                        worked_hours_on_holidays = hours_worked,
                        jf_value = 1       
                    WHERE employee_id = $1 AND date = $2;
                `, [employeeId, date]);
            } else if (is_holiday) {
                await client.query(`
                    UPDATE attendance_summary
                    SET 
                        penalisable = 0,
                        missed_hour = 0,
                        status = 'jf_lose',
                        get_holiday = TRUE,
                        worked_hours_on_holidays = hours_worked,
                        isholidays = TRUE     
                    WHERE employee_id = $1 AND date = $2;
                `, [employeeId, date]);
            }
        } else {
            console.log(`ℹ️ Aucun jour férié trouvé pour ${employeeId} à la date ${date}`);
        }

    } catch (error) {
        console.error(`❌ Erreur lors du traitement du check du jour férié:`, error.message);
        throw error;
    } finally {
        client.release();
    }
}

// Gestion des Indisponibilités
async function employeeUnvailable(date, employeeId, employee_innerID) {
    if (!Number.isInteger(Number(employeeId)) || isNaN(new Date(date).getTime())) {
        throw new Error(`Paramètres invalides: employeeId=${employeeId}, date=${date}`);
    }

    const client = await pool.connect();
   

    try {
        console.log(`🚫 Check Indisponibilité pour l'empmloyé ${employeeId} à la date ${date}`);

        // verifier si ce jour est un layoff
        const layoffQuery = `
            SELECT start_date, end_date, type
            FROM layoff
            WHERE start_date <= $1 AND end_date >= $1 AND is_purged = FALSE AND employee_id = $2
        `;
        const layoffResult = await client.query(layoffQuery, [date, employee_innerID]);


        if (layoffResult.rowCount > 0) {

            const layof_type = layoffResult.rows[0].type;

            console.log(`🚫 Indisponibilité pour ${employeeId} à la date ${date}`);


            if (layof_type === 'conge') {
                await client.query(`
                    UPDATE attendance_summary
                    SET 
                        status = 'conge',
                        is_conge = TRUE,
                        get_holiday = TRUE,
                        nbr_absence = 1,
                        jc_value = 1       
                    WHERE employee_id = $1 AND date = $2;
                `, [employeeId, date]);
            } else if (layof_type === 'map') {
                await client.query(`
                    UPDATE attendance_summary
                    SET
                        status = 'map',
                        nbr_absence = 1,
                        islayoff = TRUE  
                    WHERE employee_id = $1 AND date = $2;
                `, [employeeId, date]);
            } else if (layof_type === 'accident') {
                await client.query(`
                    UPDATE attendance_summary
                    SET 
                        status = 'accident',
                        nbr_absence = 1,
                        is_accident = TRUE     
                    WHERE employee_id = $1 AND date = $2;
                `, [employeeId, date]);

            } else if (layof_type === 'cg_maladie') {
                await client.query(`
                    UPDATE attendance_summary
                    SET 
                        status = 'cg_maladie',
                        nbr_absence = 1,
                        is_maladie = TRUE  
                    WHERE employee_id = $1 AND date = $2;
                `, [employeeId, date]);
            }
            else if (layof_type === 'rdv_medical') {
                await client.query(`
                    UPDATE attendance_summary
                    SET                   
                        status = 'rdv_medical',
                        nbr_absence = 1,
                        get_holiday = TRUE,
                        is_congex = TRUE,
                        jcx_value = 1       
                    WHERE employee_id = $1 AND date = $2;
                `, [employeeId, date]);
            } else {
                await client.query(`
                    UPDATE attendance_summary
                    SET 
                        status = 'cg_exp',
                        nbr_absence = 1,
                        get_holiday = TRUE,
                        is_congex = TRUE,
                        jcx_value = 1       
                    WHERE employee_id = $1 AND date = $2;
                `, [employeeId, date]);
            } 
        } else {
            console.log(`ℹ️ Aucun Layoff trouvé pour ${employeeId} à la date ${date}`);
        }

    } catch (error) {
        console.error(`❌ Erreur lors du traitement du check du Layoff:`, error.message);
        throw error;
    } finally {
        client.release();
    }
}

// Mettre à jour attendance_summary selon le shift de l'employé OK
async function employeeWorkShift(date, employeeId, employee_innerID) {
    if (!Number.isInteger(Number(employeeId)) || isNaN(new Date(date).getTime())) {
        throw new Error(`Paramètres invalides: employeeId=${employeeId}, date=${date}`);
    }

    const client = await pool.connect();

    try {
        console.log(`📅 Traitement du shift pour employé ${employeeId} à la date ${date}`);

        const shiftQuery = `
        SELECT ews.work_shift_id, ws.*
        FROM employee_work_shifts ews
        JOIN work_shifts ws ON ews.work_shift_id = ws.id
        WHERE ews.employee_id = $1 
        AND ews.start_date <= $2 
        AND (ews.end_date IS NULL OR ews.end_date >= $2);
    `;
        
        const result = await client.query(shiftQuery, [employee_innerID, date]);
        if (result.rowCount === 0) {
            console.log(`❌ Aucun shift trouvé pour ${employeeId} à la date ${date}`);
            return;
        }
        
        const shift = result.rows[0];
        const dayOfWeek = new Date(date).toLocaleString('en-US', { weekday: 'long' }).toLowerCase();
        
        if (shift[`${dayOfWeek}_off`]) {
            console.log(`🚫 Jour de repos pour ${employeeId} à la date ${date}`);
            return;
        }
        
        const is_dayoff = shift[`${dayOfWeek}_off`]
        const startTime = shift[`${dayOfWeek}_start`];
        const endTime = shift[`${dayOfWeek}_end`];
        const breakMinutes = shift[`${dayOfWeek}_break`] || 0;
        const break_duration = breakMinutes / 60;
        let workDuration = 0;

        if (!is_dayoff) {
             workDuration = ((new Date(`1970-01-01T${endTime}`) - new Date(`1970-01-01T${startTime}`)) / 3600000) - (breakMinutes / 60);
        }
      
        await client.query(`
            UPDATE attendance_summary
            SET 
                status = 'absent',
                penalisable = $3,
                missed_hour = $3,
                normal_hours = $3,
                nbr_absence = 1,
                getin_ref = $4,
                break_duration = $5,
                getout_ref = $6
            WHERE employee_id = $1 AND date = $2 AND isholidays = FALSE AND is_conge = FALSE AND islayoff = FALSE;
        `, [employeeId, date, workDuration, startTime, break_duration, endTime]);
        
        console.log(`✅ Shift mis à jour pour ${employeeId} à la date ${date}`);
    } catch (error) {
        console.error(`❌ Erreur lors du traitement du shift:`, error.message);
        throw error;
    } finally {
        client.release();
    }
}
// Fonction pour traiter les shifts de nuit ok
async function processNightShifts(employeeId) {
    // Validation de l'input
    if (!Number.isSafeInteger(parseInt(employeeId, 10))) {
        throw new Error(`ID employé invalide: ${employeeId}`);
    }
    const client = await pool.connect();
    try {
        console.log(`🌙 Traitement des shifts de nuit pour l'employé ${employeeId}`);

        // Récupération des paires IN/OUT optimisée
        const nightShiftsQuery = `
            WITH ordered_punches AS (
                SELECT id, employee_id, punch_time, punch_type,
                       LAG(id) OVER (PARTITION BY employee_id ORDER BY punch_time) AS in_id,
                       LAG(punch_time) OVER (PARTITION BY employee_id ORDER BY punch_time) AS in_time
                FROM attendance_records
                WHERE employee_id = $1
            )
            SELECT in_id, id AS out_id, in_time, punch_time AS out_time,
                   DATE(in_time) AS shift_date,
                   LEAST(EXTRACT(EPOCH FROM (punch_time - in_time)) / 3600, 8) AS night_hours
            FROM ordered_punches
            WHERE punch_type = 'OUT' AND in_time IS NOT NULL
              AND (EXTRACT(HOUR FROM in_time) >= 21 OR EXTRACT(HOUR FROM punch_time) < 6)
            ORDER BY shift_date;
        `;

        const nightShifts = await client.query(nightShiftsQuery, [employeeId]);
        console.log(`🔍 ${nightShifts.rows.length} shifts de nuit trouvés`);

        // Mise à jour des summaries existants
        for (const shift of nightShifts.rows) {
            const nightHours = parseFloat(shift.night_hours) || 0;
            if (nightHours > 24) {
                console.warn(`⚠️ Heures de nuit anormales (${nightHours}h) pour employé ${employeeId} le ${shift.shift_date}`);
                continue;
            }

            const durationSec = Math.floor((new Date(shift.out_time) - new Date(shift.in_time)) / 1000);
            const in_time_night = new Date(shift.in_time).toTimeString().slice(0, 5);
            const out_time_night = new Date(shift.out_time).toTimeString().slice(0, 5);

            try {
                await client.query(`
                    UPDATE attendance_summary
                    SET night_getin = $3, night_getout = $4,
                        has_night_shift = $5, night_hours = $6, 
                        night_worked = ($7::text || ' seconds')::interval,
                        status = 'night-shift',
                        get_holiday = TRUE,
                        penalisable = GREATEST(penalisable - $6, 0),
                        updated_at = NOW()
                    WHERE employee_id = $1 
                      AND date = $2 
                      AND isholidays = FALSE 
                      AND is_conge = FALSE 
                      AND islayoff = FALSE;
                `, [employeeId, shift.shift_date, in_time_night, out_time_night, nightHours > 0, nightHours, durationSec.toString()]);

                console.log(`📅 ${shift.shift_date}: ${nightHours.toFixed(2)}h (${shift.in_time} → ${shift.out_time})`);
            } catch (dbError) {
                console.error(`❌ Erreur DB pour ${employeeId} (${shift.shift_date}):`, dbError);
                throw dbError;
            }
        }

        console.log(`✅ Traitement des shifts de nuit terminé pour employé ${employeeId}`);
    } catch (error) {
        console.error(`❌ Erreur majeure pour employé ${employeeId}:`, error);
        throw error;
    } finally {
        client.release();
    }
}

// Fonction helper pour vérifier l'intégrité des shifts
async function verifyShiftIntegrity(employeeId, punches) {
    let lastIn = null;
    let errors = [];

    for (const punch of punches) {
        if (punch.punch_type === 'IN') {
            if (lastIn) {
                errors.push(`Doublon IN détecté: ${punch.punch_time}`);
            }
            lastIn = punch;
        } else if (punch.punch_type === 'OUT') {
            if (!lastIn) {
                errors.push(`OUT sans IN précédent: ${punch.punch_time}`);
            }
            lastIn = null;
        }
    }

    if (errors.length > 0) {
        await pool.query(`
            INSERT INTO attendance_alerts (employee_id, message, alert_time)
            VALUES ($1, $2, NOW())`,
            [employeeId, `Problèmes de cohérence: ${errors.join('; ')}`]
        );
    }
}
// Fonction pour traiter les shifts reguliers OK
async function processRegularShifts(employeeId) {
    // Constants for business rules
    const REGULAR_SHIFT_START_MINUTES = 6 * 60 + 16; // 6:16 AM (06:16)
    const REGULAR_SHIFT_END_MINUTES = 20 * 60 + 59;   // 8:59 PM (20:59)
    const MIN_WORK_HOURS = 0; // Minimum worked hours can't be negative
    const HOURS_PRECISION = 2; // Decimal places for hours calculations

    if (!Number.isInteger(Number(employeeId))) {
        throw new Error(`Invalid employee ID: ${employeeId}`);
    }

    const client = await pool.connect();
    try {
        console.log(`🌞 Processing regular shifts for employee ${employeeId}`);

        // 1. Fetch all regular shifts (IN-OUT pairs)
        const regularShiftsQuery = `
            WITH ordered_punches AS (
                SELECT 
                    id, 
                    employee_id, 
                    punch_time, 
                    punch_type,
                    LAG(id) OVER (PARTITION BY employee_id, DATE(punch_time) ORDER BY punch_time) AS prev_id,
                    LAG(punch_time) OVER (PARTITION BY employee_id, DATE(punch_time) ORDER BY punch_time) AS prev_time,
                    LAG(punch_type) OVER (PARTITION BY employee_id, DATE(punch_time) ORDER BY punch_time) AS prev_type
                FROM attendance_records
                WHERE employee_id = $1
                  AND EXTRACT(HOUR FROM punch_time) * 60 + EXTRACT(MINUTE FROM punch_time) 
                      BETWEEN $2 AND $3
            )
            SELECT 
                prev_id AS in_id, 
                id AS out_id, 
                prev_time AS getin_time, 
                punch_time AS getout_time,
                DATE(prev_time) AS shift_date,
                EXTRACT(EPOCH FROM (punch_time - prev_time)) / 3600 AS raw_worked_hours
            FROM ordered_punches
            WHERE punch_type = 'OUT' 
              AND prev_type = 'IN' 
              AND prev_time IS NOT NULL
            ORDER BY shift_date;
        `;

        const shifts = await client.query(regularShiftsQuery, [
            employeeId, 
            REGULAR_SHIFT_START_MINUTES, 
            REGULAR_SHIFT_END_MINUTES
        ]);

        // 2. Process each shift
        for (const shift of shifts.rows) {
            try {
                // Format times safely
                const formatTime = (dateTime) => {
                    if (!dateTime || isNaN(new Date(dateTime).getTime())) return null;
                    const d = new Date(dateTime);
                    return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
                };
                

                const isValidTime = (time) => time && !isNaN(new Date(time).getTime());

                const getin = isValidTime(shift.getin_time) ? formatTime(shift.getin_time) : null;
                const getout = isValidTime(shift.getout_time) ? formatTime(shift.getout_time) : null;
                let workedHours = parseFloat(shift.raw_worked_hours) || 0;

                // 3. Get shift summary data
                const summaryQuery = `
                    SELECT 
                        getin_ref, 
                        getout_ref,
                        break_duration
                    FROM attendance_summary
                    WHERE employee_id = $1 
                      AND date = $2
                      AND isholidays = FALSE 
                      AND is_conge = FALSE 
                      AND islayoff = FALSE
                    LIMIT 1;
                `;
                const summaryResult = await client.query(summaryQuery, [employeeId, shift.shift_date]);
                
                if (summaryResult.rows.length === 0) continue;
                
                const summary = summaryResult.rows[0];
                const getin_ref = isValidTime(summary.getin_ref) ? formatTime(summary.getin_ref) : null;
                const getout_ref = isValidTime(summary.getout_ref) ? formatTime(summary.getout_ref) : null;
                const breakDuration = parseFloat(summary.break_duration) || 0;

                // 4. Calculate authorization periods
                const authorizationQuery = `
                    SELECT 
                        MIN(CAST(punch_time AS TIME)) AS autoriz_getin_time, 
                        MAX(CAST(punch_time AS TIME)) AS autoriz_getout_time
                    FROM attendance_records
                    WHERE employee_id = $1 
                      AND DATE(punch_time) = $2
                      AND punch_type IN ('IN', 'OUT')
                      AND CAST(punch_time AS TIME) > $3::TIME 
                      AND CAST(punch_time AS TIME) < $4::TIME
                    GROUP BY DATE(punch_time);
                `;

                const autorizTimes = await client.query(authorizationQuery, [
                    employeeId, 
                    shift.shift_date, 
                    getin, 
                    getout
                ]);

                const autoriz_getin = autorizTimes.rows[0]?.autoriz_getin_time 
                    ? formatTime(new Date(autorizTimes.rows[0].autoriz_getin_time)) 
                    : null;
                const autoriz_getout = autorizTimes.rows[0]?.autoriz_getout_time 
                    ? formatTime(new Date(autorizTimes.rows[0].autoriz_getout_time)) 
                    : null;
                    

                // 5. Calculate adjusted worked hours
                if (getin && getin_ref && getin < getin_ref) {
                    workedHours = (new Date(`1970-01-01T${getout}:00`) - 
                                 new Date(`1970-01-01T${getin_ref}:00`)) / 3600000;
                }

                // Subtract authorization time if exists
                if (autoriz_getin && autoriz_getout) {
                    const authDuration = (new Date(`1970-01-01T${autoriz_getout}:00`) - 
                                        new Date(`1970-01-01T${autoriz_getin}:00`)) / 3600000;
                    workedHours = Math.max(workedHours - authDuration, MIN_WORK_HOURS);
                }

                // Subtract break time
                workedHours = parseFloat(
                    Math.max(workedHours - breakDuration, MIN_WORK_HOURS).toFixed(HOURS_PRECISION)
                );
                

                // 7. Update the summary record
                await client.query(`
                    UPDATE attendance_summary
                    SET 
                        getin = $3::TIME,
                        getout = $4::TIME,
                        autoriz_getin = $5::TIME,
                        autoriz_getout = $6::TIME,
                        get_holiday = TRUE,
                        nbr_absence = 0,
                        status = CASE
                            WHEN $3 IS NULL THEN 'absent'
                            WHEN getin_ref IS NULL THEN 'present'
                            WHEN $3::TIME <= getin_ref THEN 'present'
                            ELSE 'retard'
                        END,
                        nbr_retard = CASE
                            WHEN $3::TIME <= getin_ref THEN 0
                            WHEN $3::TIME > getin_ref THEN 1
                        END,  
                        nbr_depanti = CASE
                            WHEN $4::TIME < getout_ref THEN 1
                            ELSE 0
                        END,    
                        hours_worked = $7::NUMERIC,
                        sunday_hour = CASE
                        WHEN is_sunday = TRUE THEN $7::NUMERIC
                        ELSE 0
                        END,
                        sup_hour = CASE
                        WHEN is_saturday = TRUE THEN GREATEST($7::NUMERIC - normal_hours::NUMERIC, 0 )
                        ELSE 0
                        END,
                        missed_hour = GREATEST(normal_hours::NUMERIC - $7::NUMERIC, 0 ),
                        penalisable = GREATEST(normal_hours::NUMERIC - $7::NUMERIC, 0 ),
                        updated_at = NOW()
                    WHERE employee_id = $1 AND date = $2
                    AND isholidays = FALSE AND is_conge = FALSE AND islayoff = FALSE 
                    AND is_congex = FALSE
                    AND is_maladie = FALSE AND is_accident = FALSE;
                `, [
                    employeeId, 
                    shift.shift_date, 
                    getin || null,  // <-- S'assurer que `null` est utilisé au lieu de `NaN`
                    getout || null, 
                    autoriz_getin || null, 
                    autoriz_getout || null, 
                    workedHours || 0
                ]);
                

            } catch (shiftError) {
                console.error(`❌ Error processing shift on ${shift.shift_date}:`, shiftError.message);
                // Continue with next shift even if one fails
                continue;
            }
        }

        console.log(`✅ Completed processing for employee ${employeeId}`);
    } catch (error) {
        console.error(`❌ Critical error processing employee ${employeeId}:`, error.message);
        throw error;
    } finally {
        client.release();
    }
}

// Fonction attendance_summary
async function attendanceSummary(employeeId,employee_innerID, date) {
    try {
        console.log(`📅 Traitement de la présence pour l'employé ${employeeId} à la date ${date}`);

        // Appel de la fonction d'initialisation de l'attendance summary
        await initAttendanceSummary(employeeId, date);

        // Appel de la fonction pour traiter les shifts de travail de l'employé
        await employeeWorkShift(date, employeeId, employee_innerID);

         // Appel de la fonction pour traiter les indisponibilités de l'employé
         await employeeUnvailable(date, employeeId, employee_innerID);

         // Appel de la fonction pour vérifier les jours fériés
         await employeeHoliday(date, employeeId);

        // Appel de la fonction pour traiter les shifts réguliers de l'employé
        await processRegularShifts(employeeId);

        // Appel de la fonction pour traiter les shifts de nuit de l'employé
        await processNightShifts(employeeId);


        console.log(`✅ Traitement de l'attendance summary terminé pour l'employé ${employeeId} à la date ${date}`);
    } catch (error) {
        console.error(`❌ Erreur lors du traitement de l'attendance summary pour l'employé ${employeeId} à la date ${date}:`, error);
        throw error;
    }
}

// fonction pour traiter les pointages sur une période donnée
async function processMonthlyAttendance() {
    const startDate = '2025-03-23'; // Première date de la période (exemple: 1er mars 2025)
    const endDate = '2025-03-26'; // Dernière date de la période (exemple: 31 mars 2025)

    const client = await pool.connect();
    try {
        console.log(`📅 Traitement des résumés d'attendance pour tous les employés entre ${startDate} et ${endDate}`);

        // Récupérer tous les employés
        const employeesQuery = 'SELECT id, attendance_id FROM employees';
        const employeesResult = await client.query(employeesQuery);

        // Vérifier qu'il y a des employés
        if (employeesResult.rows.length === 0) {
            console.log('Aucun employé trouvé.');
            return;
        }

        // Boucle sur chaque employé et application de la fonction attendanceSummary pour chaque date de la période
        for (let currentDate = new Date(startDate); currentDate <= new Date(endDate); currentDate.setDate(currentDate.getDate() + 1)) {
            const dateString = currentDate.toISOString().split('T')[0]; // Format YYYY-MM-DD
            console.log(`🌟 Traitement des présences pour la date ${dateString}`);

            // Appliquer la fonction attendanceSummary pour chaque employé à la date spécifique
            for (const employee of employeesResult.rows) {
                try {
                    await attendanceSummary(employee.attendance_id,employee.id, dateString);
                } catch (error) {
                    console.error(`❌ Erreur lors du traitement de l'attendance pour l'employé ${employee.attendance_id} à la date ${dateString}:`, error);
                }
            }
        }

        console.log(`✅ Traitement des résumés d'attendance terminé pour tous les employés.`);

    } catch (error) {
        console.error('❌ Erreur lors du traitement des résumés d\'attendances mensuels:', error);
        throw error;
    } finally {
        client.release();
    }
}

// Version pour traiter tous les employés
async function processAllNightShifts() {
    console.log('[Début] Traitement des shifts de nuit pour tous les employés');
    
    try {
        // 1. Requête SQL pour recuperer les pointages nuit
        const { rows } = await pool.query(`
            SELECT DISTINCT employee_id
            FROM attendance_records
            WHERE (
                -- Pointages à partir de 20h30
                (EXTRACT(HOUR FROM punch_time) = 20 AND EXTRACT(MINUTE FROM punch_time) >= 30)
                OR
                (EXTRACT(HOUR FROM punch_time) > 20)
                OR
                -- Pointages jusqu'à 6h10
                (EXTRACT(HOUR FROM punch_time) < 6)
                OR
                (EXTRACT(HOUR FROM punch_time) = 6 AND EXTRACT(MINUTE FROM punch_time) <= 10)
            )
            AND punch_type IS NOT NULL
            ORDER BY employee_id`
        );

        console.log(`👥 ${rows.length} employés avec des pointages de nuit à traiter`);

        // 2. Traitement par lots pour améliorer les performances
        const BATCH_SIZE = 5;
        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);
            console.log(`\n--- Traitement du lot ${Math.floor(i/BATCH_SIZE) + 1}/${Math.ceil(rows.length/BATCH_SIZE)} ---`);
            
            await Promise.all(batch.map(async (row) => {
                try {
                    await processNightShifts(row.employee_id);
                } catch (err) {
                    console.error(`❌ Erreur sur employé ${row.employee_id}:`, err.message);
                    await pool.query(`
                        UPDATE attendance_summary
                        SET needs_review = TRUE,
                            review_reason = $1
                        WHERE employee_id = $2
                        AND has_night_shift = TRUE`,
                        [`Erreur traitement automatique: ${err.message.slice(0, 100)}`, row.employee_id]
                    );
                }
            }));
        }

        // 3. Requête de nettoyage corrigée
        const cleanupRes = await pool.query(`
            UPDATE attendance_summary s
            SET has_night_shift = FALSE,
                night_hours = 0,
                night_worked = NULL
            WHERE has_night_shift = TRUE
            AND NOT EXISTS (
                SELECT 1 FROM attendance_records r
                WHERE r.employee_id = s.employee_id
                AND DATE(r.punch_time) = s.date
                AND (
                    (EXTRACT(HOUR FROM r.punch_time) = 20 AND EXTRACT(MINUTE FROM r.punch_time) >= 30)
                    OR
                    (EXTRACT(HOUR FROM r.punch_time) > 20)
                    OR
                    (EXTRACT(HOUR FROM r.punch_time) < 6)
                    OR
                    (EXTRACT(HOUR FROM r.punch_time) = 6 AND EXTRACT(MINUTE FROM r.punch_time) <= 10)
                )  -- ✅ Cette parenthèse ferme correctement le bloc précédent
                AND r.punch_type IS NOT NULL
            )
        `);
        

        console.log(`🧹 ${cleanupRes.rowCount} entrées obsolètes nettoyées`);
        console.log('[Fin] Traitement complet des shifts de nuit');
    } catch (error) {
        console.error('[ERREUR GLOBALE]', error);
        throw error;
    }
}


// Vérification des cohérences des shifts de nuit
async function verifyNightShiftConsistency(employeeId) {
    // Détecter les incohérences spécifiques aux shifts de nuit
    const issues = await pool.query(`
        WITH night_punches AS (
            SELECT *, 
                   EXTRACT(HOUR FROM punch_time) as hour,
                   LAG(punch_type) OVER (ORDER BY punch_time) as prev_type
            FROM attendance_records
            WHERE employee_id = $1
              AND (EXTRACT(HOUR FROM punch_time) >= 21 
                   OR EXTRACT(HOUR FROM punch_time) < 6)
        )
        SELECT id FROM night_punches
        WHERE (hour < 6 AND punch_type = 'IN' AND prev_type = 'IN')
           OR (hour >= 21 AND punch_type = 'OUT' AND prev_type = 'OUT')`,
        [employeeId]
    );

    // Correction automatique + flag pour review
    for (const issue of issues.rows) {
        await pool.query(`
            UPDATE attendance_records
            SET punch_type = CASE WHEN punch_type = 'IN' THEN 'OUT' ELSE 'IN' END,
                needs_review = TRUE
            WHERE id = $1`,
            [issue.id]
        );
    }
}


async function updateAttendanceSummary(employeeId) {
    const client = await pool.connect();
    try {
        console.log(`📊 Mise à jour du summary pour l'employé ${employeeId}`);

        // 1. Récupérer tous les pointages classifiés avec les heures de nuit
        const punches = await client.query(`
            SELECT 
                id,
                punch_time,
                punch_type,
                EXTRACT(HOUR FROM punch_time) as hour
            FROM classified_punches
            WHERE employee_id = $1
            ORDER BY punch_time`,
            [employeeId]
        );

        // 2. Récupérer tous les shifts programmés
        const scheduledShifts = await client.query(`
            SELECT 
                ws.*,
                d.date,
                EXTRACT(DOW FROM d.date) as day_of_week,
                (d.date)::text as date_str
            FROM 
                (SELECT generate_series(
                    CURRENT_DATE - INTERVAL '30 days', 
                    CURRENT_DATE + INTERVAL '30 days', 
                    '1 day'
                )::date AS date) d
            JOIN employee_work_shifts ews ON ews.employee_id = $1
            JOIN work_shifts ws ON ews.work_shift_id = ws.id
            WHERE d.date BETWEEN ws.start_date AND ws.end_date
            ORDER BY d.date`,
            [employeeId]
        );

        // 3. Traiter chaque jour de travail programmé
        for (const shift of scheduledShifts.rows) {
            const date = shift.date;
            const dateStr = shift.date_str;
            const dayOfWeek = shift.day_of_week; // 0=Dim, 1=Lun, ..., 6=Sam
            const isWeekend = dayOfWeek === 0 || dayOfWeek === 6;
            const isSunday = dayOfWeek === 0;
            const isSaturday = dayOfWeek === 6;
            
            // Trouver les pointages pour ce jour
            const dayPunches = punches.rows.filter(p => {
                const punchDate = new Date(p.punch_time).toISOString().split('T')[0];
                return punchDate === dateStr;
            });

            // Vérifier si l'employé a pointé ce jour-là
            const hasPunched = dayPunches.length > 0;
            const isOffDay = shift[`${dayOfWeek}_off`];
            const scheduledHours = shift[`${dayOfWeek}_end`] - shift[`${dayOfWeek}_start`] - (shift[`${dayOfWeek}_break`] / 60);

            // Variables de calcul
            let status = 'present';
            let regularHours = 0;
            let supHours = 0;
            let sundayHours = 0;
            let missedHours = 0;
            let getin = null;
            let getout = null;

            if (!isOffDay) {
                if (!hasPunched) {
                    // Cas d'absence complète
                    status = 'absent';
                    missedHours = scheduledHours;
                } else {
                    // Traitement des pointages existants
                    const inPunches = dayPunches.filter(p => p.punch_type === 'IN').sort((a, b) => a.punch_time - b.punch_time);
                    const outPunches = dayPunches.filter(p => p.punch_type === 'OUT').sort((a, b) => a.punch_time - b.punch_time);

                    if (inPunches.length > 0 && outPunches.length > 0) {
                        getin = inPunches[0].punch_time;
                        getout = outPunches[outPunches.length - 1].punch_time;
                        
                        // Calcul des heures travaillées (en heures)
                        const totalWorkedHours = (new Date(getout) - new Date(getin)) / (1000 * 60 * 60);
                        
                        // Soustraction de la pause si configurée
                        const workedHours = shift[`${dayOfWeek}_break`] > 0 
                            ? totalWorkedHours - (shift[`${dayOfWeek}_break`] / 60)
                            : totalWorkedHours;

                        // Calcul des heures manquées
                        missedHours = Math.max(scheduledHours - workedHours, 0);
                        
                    } else {
                        // Cas où il manque des pointages IN ou OUT
                        status = 'incomplete';
                        missedHours = scheduledHours;
                    }
                }
            }

            // 4. Mise à jour du summary
            await client.query(`
                INSERT INTO attendance_summary (
                    employee_id, 
                    date, 
                    status,
                    regular_getin,
                    regular_getout,
                    regular_hours,
                    sup_hours,
                    missed_hours,
                    has_regular_shift,
                    needs_review
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (employee_id, date) DO UPDATE SET
                    status = EXCLUDED.status,
                    regular_getin = EXCLUDED.regular_getin,
                    regular_getout = EXCLUDED.regular_getout,
                    regular_hours = EXCLUDED.regular_hours,
                    sup_hours = EXCLUDED.sup_hours,
                    has_regular_shift = EXCLUDED.has_regular_shift,
                    needs_review = EXCLUDED.needs_review,
                    updated_at = NOW()
                WHERE attendance_summary.date = EXCLUDED.date`,
                [
                    employeeId,
                    date,
                    status,
                    getin ? formatTimeToHHMM(getin) : null,
                    getout ? formatTimeToHHMM(getout) : null,
                    regularHours,
                    supHours,
                    missedHours,
                    hasPunched, // has_regular_shift = true si pointages
                    dayPunches.some(p => p.needs_review)
                ]
            );

            console.log(`📅 ${dateStr} (${getDayName(dayOfWeek)}): 
                Statut: ${status} | 
                Régulier: ${regularHours.toFixed(2)}h | 
                Sup: ${supHours.toFixed(2)}h | 
                Manquées: ${missedHours.toFixed(2)}h`);
        }

        console.log(`✅ Summary mis à jour pour l'employé ${employeeId}`);
    } catch (error) {
        console.error(`❌ Erreur lors de la mise à jour du summary pour ${employeeId}:`, error);
        throw error;
    } finally {
        client.release();
    }
}

// Helper functions
function formatTimeToHHMM(dateTime) {
    if (!dateTime) return null;
    const date = new Date(dateTime);
    return `${date.getHours().toString().padStart(2, '0')}:${date.getMinutes().toString().padStart(2, '0')}`;
}

function getDayName(dayOfWeek) {
    const days = ['Dimanche', 'Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi'];
    return days[dayOfWeek];
}

// Fonctions helper
function calculateDailyHours(punches, shift, dayOfWeek) {
    let workedMinutes = 0;
    let nightMinutes = 0;
    let outMinutes = 0;
    let currentIn = null;

    for (let i = 0; i < punches.length; i++) {
        const punch = punches[i];
        
        if (punch.punch_type === 'IN') {
            currentIn = punch.punch_time;
        } 
        else if (punch.punch_type === 'OUT' && currentIn) {
            const start = new Date(currentIn);
            const end = new Date(punch.punch_time);
            const durationMinutes = (end - start) / (1000 * 60);
            
            // Calcul des heures de nuit
            nightMinutes += calculateNightMinutes(start, end);
            
            // Détection des sorties autorisées (< 2h et pas dernier OUT)
            if (durationMinutes < 120 && i < punches.length - 1) {
                outMinutes += durationMinutes;
            } else {
                workedMinutes += durationMinutes;
            }
            
            currentIn = null;
        }
    }

    // Appliquer la pause si shift normal
    if (shift && !shift[`${dayOfWeek}_off`]) {
        workedMinutes = Math.max(workedMinutes - shift[`${dayOfWeek}_break`], 0);
    }

    return { workedMinutes, nightMinutes, outMinutes };
}

function calculateNightMinutes(start, end) {
    const NIGHT_START = 22 * 60; // 22h en minutes
    const NIGHT_END = 6 * 60;    // 6h en minutes
    
    const toMinutes = (date) => date.getHours() * 60 + date.getMinutes();
    
    const startMin = toMinutes(start);
    const endMin = toMinutes(end);
    
    // Cas traversée de minuit
    if (start.getDate() !== end.getDate()) {
        const beforeMidnight = (24 * 60) - Math.max(startMin, NIGHT_START);
        const afterMidnight = Math.min(endMin, NIGHT_END);
        return beforeMidnight + afterMidnight;
    }
    // Cas même jour
    return Math.max(Math.min(endMin, NIGHT_END) - Math.max(startMin, NIGHT_START), 0);
}

function parseTime(timeStr) {
    if (!timeStr) return 0;
    const [hours, minutes] = timeStr.split(':').map(Number);
    return hours + (minutes / 60);
}

async function processEmployeeAttendance(employeeId) {
    try {
        
        // 2. Traitement spécial shifts de nuit
        await processNightShifts(employeeId);
        
        // 3. Vérification fine
        await verifyNightShiftConsistency(employeeId);
        
        // 4. Calcul des summaries
        await updateAttendanceSummary(employeeId);
        
        // 5. Notifier si besoin de vérification humaine
        const needsReview = await pool.query(
            `SELECT 1 FROM attendance_records 
             WHERE employee_id = $1 AND needs_review LIMIT 1`,
            [employeeId]
        );

        if (needsReview.rowCount > 0) {
            await flagForManualReview(employeeId);
        }

    } catch (error) {
        console.error(`Erreur traitement pointages employé ${employeeId}:`, error);
        await flagAsProcessingError(employeeId);
    }
}

async function processAllAttendances() {
    const client = await pool.connect();
    try {
        console.log("🔍 Récupération des employés actifs...");
        
        // 1. Récupérer tous les employés actifs
        const activeEmployees = await client.query(`
            SELECT id, attendance_id FROM employees 
            WHERE is_active = TRUE
        `);

        if (activeEmployees.rows.length === 0) {
            console.log("ℹ️ Aucun employé actif trouvé");
            return;
        }

        console.log(`👥 ${activeEmployees.rows.length} employés actifs à traiter`);

        // 2. Traiter par lots avec contrôle de concurrence
        const concurrencyLimit = 5; // Nombre de traitements parallèles
        const batches = [];
        
        for (let i = 0; i < activeEmployees.rows.length; i += concurrencyLimit) {
            batches.push(activeEmployees.rows.slice(i, i + concurrencyLimit));
        }

        for (const batch of batches) {
            await Promise.all(
                batch.map(employee => 
                    processEmployeeAttendance(employee.attendance_id)
                        .catch(e => console.error(`⚠️ Erreur sur employé ${employee.id}:`, e))
            ));
            
            // Pause courte entre les batches pour éviter la surcharge
            await new Promise(resolve => setTimeout(resolve, 100));
        }

        console.log('✅ Traitement terminé pour tous les employés');
    } catch (error) {
        console.error('❌ Erreur globale du traitement:', error);
        throw error;
    } finally {
        client.release();
    }
}

async function downloadAttendance(machine) {
    const ip = machine.ip_address;
    const port = machine.port;

    const device = new Zkteco(ip, port, 5200, 5000);

    try {
        // Créer une connexion socket à l'appareil
        await device.createSocket();

        // Récupérer tous les enregistrements de pointage
        const attendanceLogs = await device.getAttendances();
        const datas = attendanceLogs.data;

        // Date de début pour filtrer les pointages (1er janvier 2025)
        const startDate = new Date('2025-01-01T00:00:00Z');

        for (const data of datas) {
            const { user_id, record_time } = data;

            // Convertir record_time en objet Date
            const punchTime = new Date(record_time);

            // Vérifier si le pointage est après le 1er janvier 2025
            if (punchTime < startDate) {
                console.log(`⏩ Pointage ignoré (avant le 1er janvier 2025) : ${punchTime.toISOString()}`);
                continue;
            }

            // Vérifier si l'employé existe
            const checkEmployee = await pool.query(
                "SELECT id FROM employees WHERE attendance_id = $1",
                [user_id]
            );

            if (checkEmployee.rowCount === 0) {
                console.warn(`⚠️ Employee ID ${user_id} does not exist. Skipping...`);
                continue;
            }

            // Vérifier si un pointage existe déjà pour cet employé dans un intervalle de 1 minute
            const checkDuplicateQuery = `
                SELECT id
                FROM attendance_records
                WHERE employee_id = $1
                AND punch_time BETWEEN $2::timestamp - INTERVAL '1 minute' AND $2::timestamp + INTERVAL '1 minute';
            `;
            const duplicateResult = await pool.query(checkDuplicateQuery, [user_id, punchTime]);

            if (duplicateResult.rowCount > 0) {
                console.log(`⏩ Pointage ignoré (doublon dans un intervalle de 1 minute) : ${punchTime}`);
                continue;
            }

            // Insérer dans la base de données, en ignorant les doublons
            const query = `
                INSERT INTO attendance_records (employee_id, shift_id, punch_time, punch_type, punch_source, device_id)
                VALUES ($1, NULL, $2, NULL, 'AUTO', $3)
                ON CONFLICT (employee_id, punch_time, device_id) DO NOTHING;
            `;

            await pool.query(query, [user_id, punchTime, machine.id]);
        }

        // Déconnecter manuellement après avoir utilisé les logs en temps réel
        await device.disconnect();
        console.log(`✅ Pointages téléchargés depuis ${ip}:${port}`);
    } catch (error) {
        console.error("Erreur lors du téléchargement des pointages:", error);
        throw error;
    }
}


// Fonction pour créer des données sur la table week_attendance à partir des données de attendance_summary
async function init_week_attendance(month = moment().startOf('month')) {
    const client = await pool.connect();
    try {
        console.log("📅 Création des semaines sur la table week_attendance_summary");

        // Fonction pour générer les semaines
        const generatePayPeriodWeeks = (month) => {
            const startDate = month.clone().subtract(1, 'month').date(26);
            const endDate = month.clone().date(25);
            let currentStart = startDate.clone();
            const generatedWeeks = [];
            let weekNum = 1;

            while (currentStart.isBefore(endDate) || currentStart.isSame(endDate, 'day')) {
                // Début de semaine (pour la 1ère semaine c'est toujours le 26)
                const weekStart = weekNum === 1 ? startDate.clone() : currentStart.clone();
                
                // Fin de semaine - comportement spécial pour la première semaine
                let weekEnd;
                if (weekNum === 1) {
                    // Trouver le prochain dimanche
                    if (weekStart.day() === 0) { // Si le 26 est déjà un dimanche
                        weekEnd = weekStart.clone();
                    } else {
                        weekEnd = weekStart.clone().day(7); // Prochain dimanche
                    }
                } else {
                    // Pour les autres semaines: 6 jours après le début
                    weekEnd = weekStart.clone().add(6, 'days');
                }

                // Ne pas dépasser la date de fin (25)
                if (weekEnd.isAfter(endDate)) {
                    weekEnd = endDate.clone();
                }

                generatedWeeks.push({
                    start: weekStart.clone(),
                    end: weekEnd.clone(),
                    weekNumber: weekNum,
                    year: weekStart.year()
                });

                currentStart = weekEnd.clone().add(1, 'day');
                weekNum++;
            }

            return generatedWeeks;
        };

        // Requête d'insertion pour la nouvelle structure
        const insertQuery = `
            INSERT INTO week_attendance_summary (
                employee_id, employee_payroll_id, employee_name,
                week_number, year, start_date, end_date,
                total_penalisable_hour, total_missed_hour, total_sup_hour, 
                total_work_hour, total_worked_hour_on_holiday, total_jc_value, total_jcx_value,
                monday_penalisable_hour, monday_missed_hour, monday_sup_hour, monday_work_hour,
                monday_worked_hour_on_holiday, monday_jc_value, monday_jcx_value, monday_is_active,
                tuesday_penalisable_hour, tuesday_missed_hour, tuesday_sup_hour, tuesday_work_hour,
                tuesday_worked_hour_on_holiday, tuesday_jc_value, tuesday_jcx_value, tuesday_is_active,
                wednesday_penalisable_hour, wednesday_missed_hour, wednesday_sup_hour, wednesday_work_hour,
                wednesday_worked_hour_on_holiday, wednesday_jc_value, wednesday_jcx_value, wednesday_is_active,
                thursday_penalisable_hour, thursday_missed_hour, thursday_sup_hour, thursday_work_hour,
                thursday_worked_hour_on_holiday, thursday_jc_value, thursday_jcx_value, thursday_is_active,
                friday_penalisable_hour, friday_missed_hour, friday_sup_hour, friday_work_hour,
                friday_worked_hour_on_holiday, friday_jc_value, friday_jcx_value, friday_is_active,
                saturday_penalisable_hour, saturday_missed_hour, saturday_sup_hour, saturday_work_hour,
                saturday_worked_hour_on_holiday, saturday_jc_value, saturday_jcx_value, saturday_is_active,
                sunday_penalisable_hour, sunday_missed_hour, sunday_sup_hour, sunday_work_hour,
                sunday_worked_hour_on_holiday, sunday_jc_value, sunday_jcx_value, sunday_is_active
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                $8, $9, $10, $11, $12, $13, $14,
                $15, $16, $17, $18, $19, $20, $21, $22,
                $23, $24, $25, $26, $27, $28, $29, $30,
                $31, $32, $33, $34, $35, $36, $37, $38,
                $39, $40, $41, $42, $43, $44, $45, $46,
                $47, $48, $49, $50, $51, $52, $53, $54,
                $55, $56, $57, $58, $59, $60, $61, $62,
                $63, $64, $65, $66, $67, $68, $69, $70
            )
            ON CONFLICT (employee_id, week_number, year) DO NOTHING
        `;

        // Récupérer tous les employés avec plus d'informations
        const { rows: employees } = await client.query('SELECT id, attendance_id, payroll_id, name FROM employees');
        if (employees.length === 0) {
            console.log('Aucun employé trouvé.');
            return;
        }

        // Déterminer le mois de paie
        const payMonth = moment().startOf('month');
        console.log(`🔄 Génération des semaines pour le mois de paie: ${payMonth.format('MMMM YYYY')}`);

        // Générer les semaines une seule fois (elles sont les mêmes pour tous les employés)
        const weeks = generatePayPeriodWeeks(payMonth);
        console.log(`📆 ${weeks.length} semaines générées`);

        // Insérer les données pour chaque employé
        for (const employee of employees) {
            try {
                // Utilisation d'une transaction par employé pour plus de sécurité
                await client.query('BEGIN');

                for (const week of weeks) {
                    const defaultValues = Array(63).fill(0);
                    // Les valeurs booléennes pour les jours actifs
                    for (let i = 22; i < 63; i += 8) {
                        defaultValues[i] = true; // Tous les jours actifs par défaut
                    }

                    await client.query(insertQuery, [
                        employee.attendance_id,
                        employee.payroll_id,
                        employee.name,
                        week.weekNumber,
                        week.year,
                        week.start.format('YYYY-MM-DD'),
                        week.end.format('YYYY-MM-DD'),
                        ...defaultValues
                    ]);
                }

                await client.query('COMMIT');
                console.log(`✓ Semaines créées pour l'employé ${employee.attendance_id}`);
            } catch (error) {
                await client.query('ROLLBACK');
                console.error(`Erreur pour l'employé ${employee.attendance_id}:`, error);
            }
        }

        console.log("✅ Données créées avec succès dans week_attendance_summary");
    } catch (error) {
        console.error("❌ Erreur lors de la création des données:", error);
        throw error;
    } finally {
        client.release();
    }
}

// Fonction pour mettre à jour les week_attendance avec les données de attendance_summary (OK)
// Fonction pour mettre à jour les week_attendance_summary avec les données de attendance_summary
async function update_week_attendance() {
    const client = await pool.connect();
    try {
        console.log("🔄 Mise à jour des totaux dans week_attendance_summary");

        // 1. Récupérer tous les employés
        const { rows: employees } = await client.query('SELECT id, attendance_id, payroll_id, name FROM employees');
        if (employees.length === 0) {
            console.log('Aucun employé trouvé.');
            return;
        }

        // 2. Requête pour récupérer les totaux par jour et par semaine pour un employé
        const getDailySummaryQuery = `
            SELECT 
                date,
                hours_worked as work_hour,
                missed_hour,
                penalisable as penalisable_hour,
                sup_hour,
                worked_hours_on_holidays as worked_hour_on_holiday,
                jc_value,
                jcx_value
            FROM attendance_summary
            WHERE employee_id = $1 AND date BETWEEN $2 AND $3
            ORDER BY date
        `;

        // 3. Requête de mise à jour complète
        const updateQuery = `
            UPDATE week_attendance_summary
            SET 
                total_penalisable_hour = $4,
                total_missed_hour = $5,
                total_sup_hour = $6,
                total_work_hour = $7,
                total_worked_hour_on_holiday = $8,
                total_jc_value = $9,
                total_jcx_value = $10,
                
                monday_penalisable_hour = $11,
                monday_missed_hour = $12,
                monday_sup_hour = $13,
                monday_work_hour = $14,
                monday_worked_hour_on_holiday = $15,
                monday_jc_value = $16,
                monday_jcx_value = $17,
                monday_is_active = $18,
                
                tuesday_penalisable_hour = $19,
                tuesday_missed_hour = $20,
                tuesday_sup_hour = $21,
                tuesday_work_hour = $22,
                tuesday_worked_hour_on_holiday = $23,
                tuesday_jc_value = $24,
                tuesday_jcx_value = $25,
                tuesday_is_active = $26,
                
                wednesday_penalisable_hour = $27,
                wednesday_missed_hour = $28,
                wednesday_sup_hour = $29,
                wednesday_work_hour = $30,
                wednesday_worked_hour_on_holiday = $31,
                wednesday_jc_value = $32,
                wednesday_jcx_value = $33,
                wednesday_is_active = $34,
                
                thursday_penalisable_hour = $35,
                thursday_missed_hour = $36,
                thursday_sup_hour = $37,
                thursday_work_hour = $38,
                thursday_worked_hour_on_holiday = $39,
                thursday_jc_value = $40,
                thursday_jcx_value = $41,
                thursday_is_active = $42,
                
                friday_penalisable_hour = $43,
                friday_missed_hour = $44,
                friday_sup_hour = $45,
                friday_work_hour = $46,
                friday_worked_hour_on_holiday = $47,
                friday_jc_value = $48,
                friday_jcx_value = $49,
                friday_is_active = $50,
                
                saturday_penalisable_hour = $51,
                saturday_missed_hour = $52,
                saturday_sup_hour = $53,
                saturday_work_hour = $54,
                saturday_worked_hour_on_holiday = $55,
                saturday_jc_value = $56,
                saturday_jcx_value = $57,
                saturday_is_active = $58,
                
                sunday_penalisable_hour = $59,
                sunday_missed_hour = $60,
                sunday_sup_hour = $61,
                sunday_work_hour = $62,
                sunday_worked_hour_on_holiday = $63,
                sunday_jc_value = $64,
                sunday_jcx_value = $65,
                sunday_is_active = $66,
                
                updated_at = CURRENT_TIMESTAMP
            WHERE employee_id = $1 AND week_number = $2 AND year = $3
        `;

        // 4. Pour chaque employé, récupérer ses semaines existantes
        for (const employee of employees) {
            try {
                await client.query('BEGIN');

                // Récupérer toutes les semaines existantes pour cet employé
                const { rows: weeks } = await client.query(
                    'SELECT id, week_number, year, start_date, end_date FROM week_attendance_summary WHERE employee_id = $1 ORDER BY start_date',
                    [employee.attendance_id]
                );

                for (const week of weeks) {
                    // Récupérer les données quotidiennes
                    const { rows: dailyData } = await client.query(getDailySummaryQuery, [
                        employee.attendance_id,
                        week.start_date,
                        week.end_date
                    ]);

                    // Initialiser les totaux
                    const totals = {
                        penalisable: 0,
                        missed: 0,
                        sup: 0,
                        work: 0,
                        holiday: 0,
                        jc: 0,
                        jcx: 0
                    };

                    // Initialiser les données par jour
                    const daysData = {
                        monday: initDayData(),
                        tuesday: initDayData(),
                        wednesday: initDayData(),
                        thursday: initDayData(),
                        friday: initDayData(),
                        saturday: initDayData(),
                        sunday: initDayData()
                    };

                    // Traiter chaque jour de données
                    for (const day of dailyData) {
                        const date = moment(day.date);
                        const dayName = getDayName(date.day());
                        
                        if (daysData[dayName]) {
                            daysData[dayName] = {
                                penalisable: parseFloat(day.penalisable_hour) || 0,
                                missed: parseFloat(day.missed_hour) || 0,
                                sup: parseFloat(day.sup_hour) || 0,
                                work: parseFloat(day.work_hour) || 0,
                                holiday: parseFloat(day.worked_hour_on_holiday) || 0,
                                jc: parseInt(day.jc_value) || 0,
                                jcx: parseInt(day.jcx_value) || 0,
                                isActive: true
                            };

                            // Mettre à jour les totaux
                            totals.penalisable += daysData[dayName].penalisable;
                            totals.missed += daysData[dayName].missed;
                            totals.sup += daysData[dayName].sup;
                            totals.work += daysData[dayName].work;
                            totals.holiday += daysData[dayName].holiday;
                            totals.jc += daysData[dayName].jc;
                            totals.jcx += daysData[dayName].jcx;
                        }
                    }

                    // Préparer les paramètres pour la requête
                    const params = [
                        employee.attendance_id,
                        week.week_number,
                        week.year,
                        totals.penalisable,
                        totals.missed,
                        totals.sup,
                        totals.work,
                        totals.holiday,
                        totals.jc,
                        totals.jcx,
                        // Lundi
                        daysData.monday.penalisable,
                        daysData.monday.missed,
                        daysData.monday.sup,
                        daysData.monday.work,
                        daysData.monday.holiday,
                        daysData.monday.jc,
                        daysData.monday.jcx,
                        daysData.monday.isActive,
                        // Mardi
                        daysData.tuesday.penalisable,
                        daysData.tuesday.missed,
                        daysData.tuesday.sup,
                        daysData.tuesday.work,
                        daysData.tuesday.holiday,
                        daysData.tuesday.jc,
                        daysData.tuesday.jcx,
                        daysData.tuesday.isActive,
                        // Mercredi
                        daysData.wednesday.penalisable,
                        daysData.wednesday.missed,
                        daysData.wednesday.sup,
                        daysData.wednesday.work,
                        daysData.wednesday.holiday,
                        daysData.wednesday.jc,
                        daysData.wednesday.jcx,
                        daysData.wednesday.isActive,
                        // Jeudi
                        daysData.thursday.penalisable,
                        daysData.thursday.missed,
                        daysData.thursday.sup,
                        daysData.thursday.work,
                        daysData.thursday.holiday,
                        daysData.thursday.jc,
                        daysData.thursday.jcx,
                        daysData.thursday.isActive,
                        // Vendredi
                        daysData.friday.penalisable,
                        daysData.friday.missed,
                        daysData.friday.sup,
                        daysData.friday.work,
                        daysData.friday.holiday,
                        daysData.friday.jc,
                        daysData.friday.jcx,
                        daysData.friday.isActive,
                        // Samedi
                        daysData.saturday.penalisable,
                        daysData.saturday.missed,
                        daysData.saturday.sup,
                        daysData.saturday.work,
                        daysData.saturday.holiday,
                        daysData.saturday.jc,
                        daysData.saturday.jcx,
                        daysData.saturday.isActive,
                        // Dimanche
                        daysData.sunday.penalisable,
                        daysData.sunday.missed,
                        daysData.sunday.sup,
                        daysData.sunday.work,
                        daysData.sunday.holiday,
                        daysData.sunday.jc,
                        daysData.sunday.jcx,
                        daysData.sunday.isActive
                    ];

                    await client.query(updateQuery, params);
                }

                await client.query('COMMIT');
                console.log(`✓ ${weeks.length} semaines mises à jour pour l'employé ${employee.attendance_id}`);
            } catch (error) {
                await client.query('ROLLBACK');
                console.error(`Erreur lors de la mise à jour pour l'employé ${employee.attendance_id}:`, error);
            }
        }

        console.log(`✅ Totaux mis à jour pour ${employees.length} employés`);
    } catch (error) {
        console.error("❌ Erreur lors de la mise à jour des totaux:", error);
        throw error;
    } finally {
        client.release();
    }
}

// Fonctions utilitaires
function initDayData() {
    return {
        penalisable: 0,
        missed: 0,
        sup: 0,
        work: 0,
        holiday: 0,
        jc: 0,
        jcx: 0,
        isActive: false
    };
}

function getDayName(dayIndex) {
    // moment.js: 0=dimanche, 1=lundi, etc.
    const days = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
    return days[dayIndex];
}


// Fonction pour créer des données sur la table week_attendance à partir des données de attendance_summary
async function init_month_attendance(month = moment().startOf('month')) {
    const client = await pool.connect();
    try {
        console.log("📅 Création des mois de paie sur la table monthly_attendance");

        const currentMonthStart = moment().startOf('month').utc();
        const startDate = moment(currentMonthStart).subtract(1, 'month').date(26);
        const endDate = moment(currentMonthStart).date(25);

       
        // Requête d'insertion pour la nouvelle structure
        const insertQuery = `
            INSERT INTO monthly_attendance (
                employee_id, payroll_id, employee_name, month_start, month_end
            ) VALUES (
                $1, $2, $3, $4::DATE, $5
            )
            ON CONFLICT (employee_id, month_start) DO NOTHING
        `;

        // Récupérer tous les employés avec plus d'informations
        const { rows: employees } = await client.query('SELECT id, attendance_id, payroll_id, name FROM employees');
        if (employees.length === 0) {
            console.log('Aucun employé trouvé.');
            return;
        }


        // Insérer les données pour chaque employé
        for (const employee of employees) {
            try {
                // Utilisation d'une transaction par employé pour plus de sécurité
                await client.query('BEGIN');

                    await client.query(insertQuery, [
                        
                        employee.attendance_id,
                        employee.payroll_id,
                        employee.name,
                        startDate,
                        endDate
                        
                    ]);
            

                await client.query('COMMIT');
                console.log(`✓ Mois de paie crée pour l'employé ${employee.attendance_id}`);
            } catch (error) {
                await client.query('ROLLBACK');
                console.error(`Erreur pour l'employé ${employee.attendance_id}:`, error);
            }
        }

        console.log("✅ Données créées avec succès dans month_attendance");
    } catch (error) {
        console.error("❌ Erreur lors de la création des données:", error);
        throw error;
    } finally {
        client.release();
    }
}

// Fonction pour remplir monthly_attendance avec les données de week_attendance (A TESTER)
async function update_monthly_attendance() {
    const client = await pool.connect();
    try {
        console.log("🔄 Mise à jour des totaux mensuels");

        // Requête d'insertion/mise à jour
        const query = `
            INSERT INTO monthly_attendance (
                employee_id, payroll_id, month_start, month_end,
                total_night_hours, total_worked_hours, total_penalisable,
                total_sup, total_sunday_hours, total_missed_hours,
                total_jf, total_jc, total_jcx, total_htjf
            )
            SELECT 
                e.attendance_id,
                e.payroll_id,
                DATE_TRUNC('month', wa.start_date - INTERVAL '5 days')::date AS month_start,
                (DATE_TRUNC('month', wa.start_date - INTERVAL '5 days') + INTERVAL '1 month' - INTERVAL '1 day')::date AS month_end,
                SUM(wa.total_night_hours) AS total_night_hours,
                SUM(wa.total_worked_hours) AS total_worked_hours,
                SUM(wa.total_penalisable) AS total_penalisable,
                SUM(wa.total_sup) AS total_sup,
                SUM(wa.total_sunday_hours) AS total_sunday_hours,
                SUM(wa.total_missed_hours) AS total_missed_hours,
                SUM(wa.total_jf) AS total_jf,
                SUM(wa.total_jc) AS total_jc,
                SUM(wa.total_jcx) AS total_jcx,
                SUM(wa.total_htjf) AS total_htjf
            FROM week_attendance wa
            JOIN employees e ON wa.employee_id = e.attendance_id
            GROUP BY e.attendance_id, e.payroll_id, DATE_TRUNC('month', wa.start_date - INTERVAL '5 days')
            ON CONFLICT (employee_id, month_start) 
            DO UPDATE SET
                payroll_id = EXCLUDED.payroll_id,
                month_end = EXCLUDED.month_end,
                total_night_hours = EXCLUDED.total_night_hours,
                total_worked_hours = EXCLUDED.total_worked_hours,
                total_penalisable = EXCLUDED.total_penalisable,
                total_sup = EXCLUDED.total_sup,
                total_sunday_hours = EXCLUDED.total_sunday_hours,
                total_missed_hours = EXCLUDED.total_missed_hours,
                total_jf = EXCLUDED.total_jf,
                total_jc = EXCLUDED.total_jc,
                total_jcx = EXCLUDED.total_jcx,
                total_htjf = EXCLUDED.total_htjf,
                updated_at = NOW()
        `;

        await client.query(query);
        console.log("✅ Totaux mensuels mis à jour avec succès");
    } catch (error) {
        console.error("❌ Erreur lors de la mise à jour des totaux mensuels:", error);
        throw error;
    } finally {
        client.release();
    }
}



const getWeeklyAttendanceByDate = async (req, res) => {
    const { date } = req.query;
    
    try {
        const query = `
            SELECT 
                wa.*, 
                e.firstname, 
                e.lastname, 
                e.matricule,
                a.hours_worked,
                a.missed_hours,
                a.sup_hour,
                a.night_hours,
                a.sunday_hour,
                a.worked_hours_on_holidays
            FROM week_attendance wa
            JOIN employees e ON wa.employee_id = e.attendance_id
            LEFT JOIN attendance_summary a ON a.employee_id = wa.employee_id 
                AND a.date = $1
            WHERE wa.start_date <= $1 AND wa.end_date >= $1
            ORDER BY e.lastname, e.firstname
        `;
        
        const { rows } = await pool.query(query, [date]);
        
        res.json({
            success: true,
            data: rows
        });
    } catch (error) {
        console.error('Error fetching daily attendance:', error);
        res.status(500).json({
            success: false,
            message: 'Erreur lors de la récupération des données journalières'
        });
    }
};


module.exports = { 
    downloadAttendance, 
    processAllAttendances, 
    processEmployeeAttendance,
    updateAttendanceSummary,
    classifyAllPunchesWithLogs,
    processAllNightShifts,
    processMonthlyAttendance,
    init_week_attendance,
    update_week_attendance,
    update_monthly_attendance,
    init_month_attendance,
};