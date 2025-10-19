# 🕒 AventuraTime Backend

**AventuraTime Backend** is the server-side application of the **AventuraTime** project.  
It provides REST APIs for managing **employee attendance, shifts, absences, and related operations**, with **multi-client support** through dynamic database connections.

---

## 🚀 Overview

- Multi-tenant architecture (each client has its own database)  
- JWT authentication with role-based access control  
- Admin, client, and user management  
- CRUD operations for items and users  
- Excel import/export utility for articles  
- Modular Express.js + Node.js design  

---

## 📁 Project Structure
backend/
├── src/
│ ├── config/ # Database configurations
│ │ ├── dbAdmin.js # Connection to admin database (users, clients)
│ │ ├── dbClient.js # Dynamic connection to a client-specific database
│ │ └── index.js
│ ├── controllers/ # Controllers for business logic
│ │ ├── adminController.js # Admin and client management
│ │ ├── clientController.js # Item and user management for clients
│ │ └── userController.js # Client user management
│ ├── models/ # Mongoose data models
│ │ ├── AdminUser.js # Admin user model (admin DB)
│ │ ├── Client.js # Client model (admin DB)
│ │ └── Item.js # Item/article model (client DB)
│ ├── routes/ # API route definitions
│ │ ├── adminRoutes.js # Routes for admins and clients
│ │ ├── clientRoutes.js # Routes for client items and users
│ │ └── userRoutes.js # Routes for user actions
│ ├── middlewares/ # Authentication and validation middleware
│ │ └── authMiddleware.js # JWT-based auth and role checking
│ ├── utils/ # Utility and helper functions
│ │ └── excelHelper.js # Excel import/export logic for items
│ ├── app.js # Express app initialization
│ └── server.js # Backend entry point
├── .env # Environment variables (DB URIs, JWT_SECRET, etc.)
├── package.json # Node.js dependencies and scripts
└── README.md # Backend documentation

