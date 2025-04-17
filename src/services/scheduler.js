const cron = require('node-cron');
const pool = require('../config/dbAventuraTime');
const { downloadAttendance, processAllAttendances, processAllNightShifts, init_week_attendance,
    processMonthlyAttendance, classifyAllPunchesWithLogs } = require('./attendanceService');


async function fetchMachines() {
    const { rows: machines } = await pool.query('SELECT ip, port FROM machines');
    return machines;
}

async function runAttendanceJob() {
    const machines = await fetchMachines();
    
    for (const machine of machines) {
        console.log(`Downloading attendance from ${machine.ip}:${machine.port}`);
        await downloadAttendance(machine);
    }
    updateAttendanceSummary(); // Mettre à jour les données après chaque Téléchargement de pointage
}

//cron.schedule('41 16 * * *', processMonthlyAttendance ); // Appel des pointage dans attendance_summary
cron.schedule('13 17 * * *', init_week_attendance ); // Appel de creation des weekly attendance

//  (Intervale de temps pour télécharger le pointage !)

// cron.schedule('0 7 */ * *', runAttendanceJob);  // 7:00 AM everyday !
// cron.schedule('0 8 */ * *', runAttendanceJob);  // 8:00 AM
// cron.schedule('0 10 */ * *', runAttendanceJob); // 10:00 AM
// cron.schedule('0 12 */ * *', runAttendanceJob); // 12:00 PM
// cron.schedule('0 15 */ * *', runAttendanceJob); // 3:00 PM


