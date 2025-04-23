const { DataTypes } = require('sequelize');
const db = require('../config/dbAventuraTime');

const Notification = db.define(
    'hr_notifications',
    {
      employee_id: { type: DataTypes.INTEGER, allowNull: true },
      notification_type: { type: DataTypes.STRING, allowNull: false },
      message: { type: DataTypes.STRING, allowNull: true },
      is_read: { type: DataTypes.BOOLEAN },
      resolved: { type: DataTypes.BOOLEAN },
     
    },
    {
      freezeTableName: true, // Pas de pluralisation
      tableName: 'hr_notifications', // Nom explicite de la table
      timestamps: false, // Désactive createdAt et updatedAt
    }
  );
  
  
  module.exports = Notification;
  
