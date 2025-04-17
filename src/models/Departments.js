const { DataTypes } = require('sequelize');
const db = require('../config/dbAventuraTime');

const Departments = db.define(
    'Departments',
    {
      code: { type: DataTypes.STRING, allowNull: false },
      name: { type: DataTypes.STRING, allowNull: false },
      responsable_id: { type: DataTypes.INTEGER, allowNull: true },
      doescontainemployees: { type: DataTypes.BOOLEAN, defaultValue: false },
    },
    {
      freezeTableName: true, // Pas de pluralisation
      tableName: 'departments', // Nom explicite de la table
      timestamps: false, // Désactive createdAt et updatedAt
    }
  );
  
  
  module.exports = Departments;
  
