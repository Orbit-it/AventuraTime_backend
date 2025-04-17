const { DataTypes } = require('sequelize');
const db = require('../config/dbAventuraTime');

const AttendanceRecord = db.define(
    'AttendanceRecord',
    {
      employee_id: { type: DataTypes.STRING, allowNull: false },
      shift_id: { type: DataTypes.INTEGER, allowNull: true },
      punch_time: { type: DataTypes.DATE, allowNull: false },
      punch_type: { type: DataTypes.STRING, allowNull: true },
      punch_source: { type: DataTypes.STRING, allowNull: true },
      device_id: { type: DataTypes.STRING, allowNull: true },
    },
    {
      freezeTableName: true, // Disable pluralization
      tableName: 'attendance_records', // Explicitly set the table name
      timestamps: false, // Disable createdAt and updatedAt fields
    }
  
    );

module.exports = AttendanceRecord;