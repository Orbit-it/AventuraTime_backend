const { DataTypes } = require('sequelize');
const db = require('../config/dbAventuraTime');

const ToolLoose = db.define(
    'ToolLoose',
    {
      employee_id: { type: DataTypes.INTEGER, allowNull: false },
      date: { type: DataTypes.DATE, allowNull: false },
      price: { type: DataTypes.NUMBER, allowNull: true },
      is_payed: { type: DataTypes.BOOLEAN, defaultValue: false },
      tool: { type: DataTypes.STRING, defaultValue: false },  // nom de l'outil
      notes: { type: DataTypes.STRING, defaultValue: false },  // notes
    },
    {
      freezeTableName: true, // Disable pluralization
      tableName: 'tool-loose', // Explicitly set the table name
      timestamps: false, // disable createdAt and updatedAt fields
    }
  
    );

module.exports = ToolLoose;