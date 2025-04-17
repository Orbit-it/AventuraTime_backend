const { Pool } = require('pg');
require('dotenv').config();
const dbAdmin = require('./src/config/dbAventuraTime');

const pool = new Pool({
  user: process.env.DB_ADMIN_USER,
  host: process.env.DB_ADMIN_HOST,
  database: process.env.DB_ADMIN_NAME,
  password: process.env.DB_ADMIN_PASSWORD,
  port: process.env.DB_ADMIN_PORT,
});

async function testConnection() {
  try {
    const client = await pool.connect();
    console.log('Connected to the database');
    const res = await client.query('SELECT NOW()');
    console.log('Database Time:', res.rows[0]);
    client.release();
  } catch (err) {
    console.error('Database connection error:', err);
  } finally {
    pool.end();
  }
}

testConnection();


dbAdmin.query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
  .then((res) => {
    console.log('Tables in the database:', res);
  })
  .catch((err) => {
    console.error('Error fetching tables:', err);
  });

