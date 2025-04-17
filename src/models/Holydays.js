const { DataTypes } = require('sequelize');
const db = require('../config/dbAventuraTime');

const Holiday = db.define(
    'Holiday',
    {
      description: { type: DataTypes.STRING, allowNull: false },
      holiday_date: { type: DataTypes.DATE, allowNull: false },
      previous_working_day: { type: DataTypes.DATE, allowNull: false },
      next_working_day: { type: DataTypes.DATE, allowNull: false },
    },
    {
      freezeTableName: true, // Disable pluralization
      tableName: 'public_holidays', // Explicitly set the table name
      timestamps: false, // disable createdAt and updatedAt fields
    }
  
    );

module.exports = Holiday;