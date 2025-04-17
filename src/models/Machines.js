const { DataTypes } = require('sequelize');
const db = require('../config/dbAventuraTime');

const Machines = db.define(
    'Machines',
    {
      ip_address: { type: DataTypes.STRING, allowNull: false },
      port: { type: DataTypes.INTEGER, allowNull: false },
      device_name: { type: DataTypes.STRING, allowNull: false },
      location: { type: DataTypes.STRING, allowNull: true },
    },
    {
      freezeTableName: true, // Pas de pluralisation
      tableName: 'attendance_devices', // Nom explicite de la table
      timestamps: false, // Désactive createdAt et updatedAt
    }
  );
  
  
  module.exports = Machines;