const cron = require('node-cron');
const pool = require('../config/dbAventuraTime');
const { downloadAttendance, processAllAttendances, processEmployeeAttendance, processAllNightShifts, init_week_attendance, update_week_attendance, init_month_attendance,
    processMonthlyAttendance, classifyAllPunchesWithLogs, verifyAndFixPunchSequence } = require('./attendanceService');
const { getMachines } = require('../controllers/machineController');
    
    

async function fetchMachines() {
    const { rows: machines } = await getMachines();
    return machines;
}

async function runAttendanceJob() {
    const machines = await fetchMachines();
    
    for (const machine of machines) {
        console.log(`Automatic Downloading attendance from ${machine.ip}:${machine.port}`);
        await downloadAttendance(machine);
    }
 
}


//cron.schedule('28 17 * * *', init_month_attendance ); // Appel de creation des weekly attendance

//cron.schedule('50 16 * * *', init_week_attendance ); // Appel de creation des weekly attendance
//cron.schedule('53 16 * * *', update_week_attendance ); // Appel de mise à jour de la table week_attendance
// cron.schedule('47 11 * * *', processAllAttendances ); // Mettre à jour les poinatages journaliers sur attendance_summary
cron.schedule('23 13 * * *', processMonthlyAttendance ); // 
 //cron.schedule('39 16 * * *', classifyAllPunchesWithLogs ); // Classification des Poinatges en IN et OUT
 //cron.schedule('16 10 * * *', verifyAndFixPunchSequence ); // Verification et correction des sequences IN / OUT et notif du Service RH


//  (Intervale de temps pour télécharger le pointage !)

 //cron.schedule('12 10 */ * *', processEmployeeAttendance);  // 7:00 AM everyday !
 cron.schedule('09 08 * * *', runAttendanceJob);  // 8:00 AM
 cron.schedule('0 10 */ * *', runAttendanceJob); // 10:00 AM
 cron.schedule('0 12 */ * *', runAttendanceJob); // 12:00 PM
 cron.schedule('0 15 */ * *', runAttendanceJob); // 3:00 PM


