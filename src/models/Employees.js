const { DataTypes } = require('sequelize');
const db = require('../config/dbAventuraTime');

const Employees = db.define(
    'Employees',
    {
      avatar: { type: DataTypes.STRING, allowNull: true },
      name: { type: DataTypes.STRING, allowNull: false },
      username: { type: DataTypes.STRING, allowNull: true },
      password: { type: DataTypes.STRING, allowNull: true },
      role: { type: DataTypes.STRING, allowNull: true },
      payroll_id: { type: DataTypes.STRING, allowNull: false },
      plafond: { type: DataTypes.NUMBER, allowNull: true },
      attendance_id: { type: DataTypes.STRING, allowNull: false },
      position: { type: DataTypes.STRING, allowNull: true },
      cnss_number: { type: DataTypes.STRING, allowNull: true },
      address: { type: DataTypes.STRING, allowNull: true },
      cin_number: { type: DataTypes.STRING, allowNull: true },
      phone_number: { type: DataTypes.STRING, allowNull: true },
      is_active: { type: DataTypes.BOOLEAN, defaultValue: true },
      department_id: { type: DataTypes.INTEGER, allowNull: true },
      birthday_date: { type: DataTypes.DATE, allowNull: true },
      hire_date: { type: DataTypes.DATE, allowNull: true },
      termination_date: { type: DataTypes.DATE, allowNull: true },
    },
    {
      freezeTableName: true, // Pas de pluralisation
      tableName: 'employees', // Nom explicite de la table
      timestamps: false, // Désactive createdAt et updatedAt
    }
  );
  
  
  module.exports = Employees;
  
