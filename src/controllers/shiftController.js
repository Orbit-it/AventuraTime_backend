const pool = require('../config/db'); // Connexion PostgreSQL

const getShifts = async (req, res) => {
    try {
        const query = `
            SELECT 
                ws.id, ws.shift_name, ws.start_date, ws.end_date,
                ws.monday_start, ws.monday_end, ws.monday_break, ws.monday_off,
                ws.tuesday_start, ws.tuesday_end, ws.tuesday_break, ws.tuesday_off,
                ws.wednesday_start, ws.wednesday_end, ws.wednesday_break, ws.wednesday_off,
                ws.thursday_start, ws.thursday_end, ws.thursday_break, ws.thursday_off,
                ws.friday_start, ws.friday_end, ws.friday_break, ws.friday_off,
                ws.saturday_start, ws.saturday_end, ws.saturday_break, ws.saturday_off,
                ws.sunday_start, ws.sunday_end, ws.sunday_break, ws.sunday_off,
                COALESCE(json_agg(wsd.department_id) FILTER (WHERE wsd.department_id IS NOT NULL), '[]') AS department_ids
            FROM work_shifts ws
            LEFT JOIN work_shift_departments wsd ON ws.id = wsd.work_shift_id
            GROUP BY ws.id
            ORDER BY ws.start_date DESC;
        `;

        const result = await pool.query(query);
        res.json(result.rows);
    } catch (error) {
        console.error("Error retrieving shifts:", error);
        res.status(500).json({ error: "Internal Server Error" });
    }
};


const createShift = async (req, res) => {
    const client = await pool.connect(); // Utilisation d'une transaction
    try {
        const { shift_name, department_ids, start_date, end_date, schedule } = req.body;

        await client.query("BEGIN"); // Début de la transaction

        // 1️⃣ Insérer le shift dans `work_shifts`
        const shiftQuery = `
            INSERT INTO work_shifts (
                shift_name, start_date, end_date,
                monday_start, monday_end, monday_break, monday_off,
                tuesday_start, tuesday_end, tuesday_break, tuesday_off,
                wednesday_start, wednesday_end, wednesday_break, wednesday_off,
                thursday_start, thursday_end, thursday_break, thursday_off,
                friday_start, friday_end, friday_break, friday_off,
                saturday_start, saturday_end, saturday_break, saturday_off,
                sunday_start, sunday_end, sunday_break, sunday_off
            ) VALUES (
                $1, $2, $3,
                $4, $5, $6, $7,
                $8, $9, $10, $11,
                $12, $13, $14, $15,
                $16, $17, $18, $19,
                $20, $21, $22, $23,
                $24, $25, $26, $27,
                $28, $29, $30, $31
            ) RETURNING id;
        `;

        const shiftValues = [
            shift_name, start_date, end_date,
            schedule.monday.start || null, schedule.monday.end || null, schedule.monday.break || 0, schedule.monday.off,
            schedule.tuesday.start || null, schedule.tuesday.end || null, schedule.tuesday.break || 0, schedule.tuesday.off,
            schedule.wednesday.start || null, schedule.wednesday.end || null, schedule.wednesday.break || 0, schedule.wednesday.off,
            schedule.thursday.start || null, schedule.thursday.end || null, schedule.thursday.break || 0, schedule.thursday.off,
            schedule.friday.start || null, schedule.friday.end || null, schedule.friday.break || 0, schedule.friday.off,
            schedule.saturday.start || null, schedule.saturday.end || null, schedule.saturday.break || 0, schedule.saturday.off,
            schedule.sunday.start || null, schedule.sunday.end || null, schedule.sunday.break || 0, schedule.sunday.off
        ];

        const shiftResult = await client.query(shiftQuery, shiftValues);
        const shiftId = shiftResult.rows[0].id;

        // 2️⃣ Associer le shift aux départements dans `work_shift_departments`
        if (department_ids.length > 0) {
            const departmentQuery = `
                INSERT INTO work_shift_departments (work_shift_id, department_id) 
                VALUES ${department_ids.map((_, i) => `($1, $${i + 2})`).join(",")}
            `;
            await client.query(departmentQuery, [shiftId, ...department_ids]);
        }

        // 3️⃣ Associer le Shift aux employés dans `employee_work_shifts` avec les dates de début et de fin
        const employeeQuery = `
            INSERT INTO employee_work_shifts (employee_id, work_shift_id, start_date, end_date)
            SELECT id, $1, $2, $3 FROM employees WHERE department_id = ANY($4::int[]);
        `;
        await client.query(employeeQuery, [shiftId,start_date, end_date, department_ids]);

        await client.query("COMMIT"); // Validation de la transaction

        res.status(201).json({ message: "Shift created successfully", shiftId });
    } catch (error) {
        await client.query("ROLLBACK"); // Annulation en cas d'erreur
        console.error("Error creating shift:", error);
        res.status(500).json({ error: "Internal Server Error" });
    } finally {
        client.release(); // Libérer la connexion
    }
};

const updateShift = async (req, res) => {
    const client = await pool.connect();
    try {
        const { id } = req.params;
        const { shift_name, department_ids, start_date, end_date, schedule } = req.body;

        await client.query("BEGIN"); // Début de la transaction

        // 1️⃣ Mise à jour du shift dans `work_shifts`
        const shiftQuery = `
            UPDATE work_shifts SET
                shift_name = $1, start_date = $2, end_date = $3,
                monday_start = $4, monday_end = $5, monday_break = $6, monday_off = $7,
                tuesday_start = $8, tuesday_end = $9, tuesday_break = $10, tuesday_off = $11,
                wednesday_start = $12, wednesday_end = $13, wednesday_break = $14, wednesday_off = $15,
                thursday_start = $16, thursday_end = $17, thursday_break = $18, thursday_off = $19,
                friday_start = $20, friday_end = $21, friday_break = $22, friday_off = $23,
                saturday_start = $24, saturday_end = $25, saturday_break = $26, saturday_off = $27,
                sunday_start = $28, sunday_end = $29, sunday_break = $30, sunday_off = $31
            WHERE id = $32;
        `;

        const shiftValues = [
            shift_name, start_date, end_date,
            schedule.monday.start || null, schedule.monday.end || null, schedule.monday.break || 0, schedule.monday.off,
            schedule.tuesday.start || null, schedule.tuesday.end || null, schedule.tuesday.break || 0, schedule.tuesday.off,
            schedule.wednesday.start || null, schedule.wednesday.end || null, schedule.wednesday.break || 0, schedule.wednesday.off,
            schedule.thursday.start || null, schedule.thursday.end || null, schedule.thursday.break || 0, schedule.thursday.off,
            schedule.friday.start || null, schedule.friday.end || null, schedule.friday.break || 0, schedule.friday.off,
            schedule.saturday.start || null, schedule.saturday.end || null, schedule.saturday.break || 0, schedule.saturday.off,
            schedule.sunday.start || null, schedule.sunday.end || null, schedule.sunday.break || 0, schedule.sunday.off,
            id
        ];

        await client.query(shiftQuery, shiftValues);

        // 2️⃣ Mise à jour des départements associés
        await client.query(`DELETE FROM work_shift_departments WHERE work_shift_id = $1`, [id]);
        if (department_ids.length > 0) {
            const departmentQuery = `
                INSERT INTO work_shift_departments (work_shift_id, department_id) 
                VALUES ${department_ids.map((_, i) => `($1, $${i + 2})`).join(",")}
            `;
            await client.query(departmentQuery, [id, ...department_ids]);
        }

        // 3️⃣ Mis à jour des employés associés
        await client.query(`DELETE FROM employee_work_shifts WHERE work_shift_id = $1`, [id]);
        const employeeQuery = `
            INSERT INTO employee_work_shifts (employee_id, work_shift_id, start_date, end_date)
            SELECT id, $1, $2, $3 FROM employees WHERE department_id = ANY($4::int[]);
        `;
        await client.query(employeeQuery, [id,start_date, end_date, department_ids]);

        await client.query("COMMIT"); // Validation de la transaction

        res.json({ message: "Shift updated successfully" });
    } catch (error) {
        await client.query("ROLLBACK"); // Annulation en cas d'erreur
        console.error("Error updating shift:", error);
        res.status(500).json({ error: "Internal Server Error" });
    } finally {
        client.release();
    }
};

const deleteShift = async (req, res) => {
    const client = await pool.connect();
    try {
        const { id } = req.params;

        await client.query("BEGIN"); // Début de la transaction

        // 1️⃣ Supprimer les liens entre le shift et les départements
        await client.query(`DELETE FROM work_shift_departments WHERE work_shift_id = $1`, [id]);

        // 2️⃣ Supprimer les liens entre le shift et les employés
        await client.query(`DELETE FROM employee_work_shifts WHERE work_shift_id = $1`, [id]);

        // 3️⃣ Supprimer le shift
        const result = await client.query(`DELETE FROM work_shifts WHERE id = $1 RETURNING *`, [id]);

        if (result.rowCount === 0) {
            throw new Error("Shift not found");
        }

        await client.query("COMMIT"); // Validation de la transaction

        res.json({ message: "Shift deleted successfully" });
    } catch (error) {
        await client.query("ROLLBACK"); // Annulation en cas d'erreur
        console.error("Error deleting shift:", error);
        res.status(500).json({ error: "Internal Server Error" });
    } finally {
        client.release();
    }
};


const assignNewEmployeeToShifts = async (req, res) => {
    const client = await pool.connect();
    try {
        const { employee_id, department_id, hire_date } = req.body;

        await client.query("BEGIN"); // Début de la transaction

        // 1️⃣ Récupérer tous les work_shifts actifs pour le département du nouvel employé
        const activeShiftsQuery = `
            SELECT ws.id, ws.start_date, ws.end_date 
            FROM work_shifts ws
            JOIN work_shift_departments wsd ON ws.id = wsd.work_shift_id
            WHERE wsd.department_id = $1 
            AND (ws.end_date IS NULL OR ws.end_date >= CURRENT_DATE)
            AND ws.start_date <= CURRENT_DATE;
        `;

        const shiftsResult = await client.query(activeShiftsQuery, [department_id]);

        // 2️⃣ Assigner le nouvel employé à tous les shifts actifs de son département
        if (shiftsResult.rows.length > 0) {
            const assignmentQuery = `
                INSERT INTO employee_work_shifts (employee_id, work_shift_id, start_date, end_date)
                VALUES ${shiftsResult.rows.map((_, i) => `($1, $${i * 3 + 2}, $${i * 3 + 3}, $${i * 3 + 4})`).join(",")}
                ON CONFLICT (employee_id, work_shift_id) DO NOTHING;
            `;

            const assignmentValues = [employee_id];
           
            shiftsResult.rows.forEach(shift => {
                // Fonction pour obtenir le dernier 26 avant la date de référence
                function getLast26thBeforeDate(referenceDate) {
                    const refDate = new Date(referenceDate);
                    const currentYear = refDate.getFullYear();
                    const currentMonth = refDate.getMonth();
                    const currentDay = refDate.getDate();
                    
                    // Si c'est le 26 ou après le 26 du mois courant, prendre le 26 du mois courant
                    if (currentDay >= 26) {
                        const result = new Date(refDate);
                        result.setDate(26);
                        return result;
                    } 
                    // Si c'est avant le 26, prendre le 26 du mois précédent
                    else {
                        const result = new Date(refDate);
                        result.setMonth(result.getMonth() - 1);
                        result.setDate(26);
                        
                        // Gestion des cas où le mois précédent n'a pas 26 jours
                        if (result.getDate() !== 26) {
                            result.setDate(0); // Dernier jour du mois précédent
                        }
                        
                        return result;
                    }
                }
                
                // Utiliser la date d'embauche comme date de référence
                const effectiveStartDate = getLast26thBeforeDate(hire_date);
                assignmentValues.push(shift.id, effectiveStartDate, shift.end_date);
            });

            await client.query(assignmentQuery, assignmentValues);
        }

        await client.query("COMMIT"); // Validation de la transaction

        res.json({ 
            message: "Employee assigned to shifts successfully",
            assigned_shifts: shiftsResult.rows.length
        });
    } catch (error) {
        await client.query("ROLLBACK"); // Annulation en cas d'erreur
        console.error("Error assigning employee to shifts:", error);
        res.status(500).json({ error: "Internal Server Error" });
    } finally {
        client.release();
    }
};






module.exports = {
    getShifts,
    createShift,
    updateShift,
    deleteShift,
    assignNewEmployeeToShifts
};

