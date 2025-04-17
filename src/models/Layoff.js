const { DataTypes } = require('sequelize');
const db = require('../config/dbAventuraTime');

const Layoff = db.define(
    'Layoff',
    {
      employee_id: { type: DataTypes.INTEGER, allowNull: false },
      type: { type: DataTypes.STRING, allowNull: false },
      start_date: { type: DataTypes.DATE, allowNull: false },
      end_date: { type: DataTypes.DATE, allowNull: false },
      nb_jour: { type: DataTypes.INTEGER, allowNull: true },
      is_purged: { type: DataTypes.BOOLEAN, defaultValue: false },
    },
    {
      freezeTableName: true, // Disable pluralization
      tableName: 'layoff', // Explicitly set the table name
      timestamps: false, // disable createdAt and updatedAt fields
    }
  
    );

module.exports = Layoff;