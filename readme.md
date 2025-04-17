backend/
├── src/
│   ├── config/               # Configuration des bases de données
│   │   ├── dbAdmin.js        # Connexion à la base de données admin (utilisateurs, clients)
│   │   ├── dbClient.js       # Connexion dynamique à la base de données d'un client spécifique
│   │   └── index.js
│   ├── controllers/          # Contrôleurs pour gérer la logique des utilisateurs et articles
│   │   ├── adminController.js # Gestion des utilisateurs/admin, clients
│   │   ├── clientController.js # Gestion des articles, utilisateurs clients
│   │   └── userController.js  # Gestion des utilisateurs (par client)
│   ├── models/               # Modèles de données
│   │   ├── AdminUser.js      # Modèle pour l'utilisateur admin dans la base admin
│   │   ├── Client.js         # Modèle pour les clients dans la base admin
│   │   └── Item.js           # Modèle pour les articles dans la base client
│   ├── routes/               # Définition des routes
│   │   ├── adminRoutes.js    # Routes pour la gestion des administrateurs et des clients
│   │   ├── clientRoutes.js   # Routes pour la gestion des articles et des utilisateurs clients
│   │   └── userRoutes.js     # Routes pour la gestion des utilisateurs
│   ├── middlewares/          # Middleware pour l'authentification et validation
│   │   └── authMiddleware.js # Authentification des utilisateurs (JWT, roles)
│   ├── utils/                # Fonctions utilitaires (ex: validation des données)
│   │   └── excelHelper.js    # Fonction pour importer des articles via Excel
│   ├── app.js                # Initialisation de l'application Express
│   └── server.js             # Point d'entrée du backend
├── .env                      # Variables d'environnement (DB, JWT_SECRET, etc.)
├── package.json              # Dépendances Node.js
└── README.md                 # Documentation du backend
