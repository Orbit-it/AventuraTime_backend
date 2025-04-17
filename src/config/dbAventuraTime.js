const { Sequelize } = require('sequelize');
require('dotenv').config();

// Connexion à la base de données admin avec Sequelize
const dbAventuratime = new Sequelize(process.env.DB_ADMIN_NAME, process.env.DB_ADMIN_USER, process.env.DB_ADMIN_PASSWORD, {
  host: process.env.DB_ADMIN_HOST,
  dialect: 'postgres',
  port: process.env.DB_ADMIN_PORT,
  logging: false, // Désactiver les logs des requêtes SQL
});




module.exports = dbAventuratime;
