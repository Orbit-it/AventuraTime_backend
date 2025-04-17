const { Pool } = require("pg");
require('dotenv').config();



// Connexion à la base de données avec pg
const pool = new Pool({
    user: process.env.DB_ADMIN_USER, 
    host: process.env.DB_ADMIN_HOST,       // Replace with your DB host
    database: process.env.DB_ADMIN_NAME,   // Replace with your DB name
    password: process.env.DB_ADMIN_PASSWORD,   // Replace with your DB password
    port: process.env.DB_ADMIN_PORT || 5432,
    max: 10,                         // Max connections
    idleTimeoutMillis: 30000,        // Close idle clients after 30s
    connectionTimeoutMillis: 2000,   // Timeout if connection fails
});



module.exports = pool;
