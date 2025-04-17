// app.js
const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const dotenv = require('dotenv');
const departmentRoutes = require('./routes/departmentRoutes');
const employeeRoutes = require('./routes/employeeRoutes');
const machineRoutes = require('./routes/machineRoutes');
const attendanceRoutes = require('./routes/attendanceRoutes');
const shiftRoutes = require('./routes/shiftRoutes');
const layoffRoutes = require('./routes/layoffRoutes');
const holidayRoutes = require('./routes/holidayRoutes'); // Routes pour les jours fériés
const authMiddleware = require('./middlewares/authMiddleware'); // Middleware d'authentification
const multer = require('multer');
const path = require('path');
const fs = require('fs');


// Créer le dossier uploads s'il n'existe pas
const uploadDir = path.join(__dirname, 'uploads');
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir, { recursive: true });
}

// Configuration de Multer
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, uploadDir);
  },
  filename: function (req, file, cb) {
    const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
    cb(null, 'avatar-' + uniqueSuffix + path.extname(file.originalname));
  }
});

const upload = multer({ 
  storage: storage,
  limits: { fileSize: 2 * 1024 * 1024 } // 2MB max
});



dotenv.config(); // Charger les variables d'environnement à partir du fichier .env

// Initialisation de l'application Express
const app = express();

// Middleware
app.use(cors()); // Permet les requêtes cross-origin
app.use(bodyParser.json()); // Pour parser le JSON dans les requêtes
app.use(bodyParser.urlencoded({ extended: true })); // Pour parser les données de formulaires URL encodées

app.use(express.json());
app.use('/uploads', express.static(uploadDir)); // Servir les fichiers statiques

// Route pour uploader l'avatar
app.post('/api/upload-avatar', upload.single('avatar'), (req, res) => {
    if (!req.file) {
      return res.status(400).json({ error: 'Aucun fichier uploadé' });
    }
  
    // Retourne le chemin relatif de l'image
    res.json({ 
      avatarPath: `/uploads/${req.file.filename}` 
    });
  });


// Routes protégées par l'authentification
//app.use('/api/admin', authMiddleware, adminRoutes); // Middleware d'authentification pour les routes admin
app.use('/api', departmentRoutes);
app.use('/api', employeeRoutes);
app.use('/api', machineRoutes);
app.use('/api', attendanceRoutes);
app.use('/api', shiftRoutes);
app.use('/api', layoffRoutes);
app.use('/api', holidayRoutes); 



// Routes non protégées, comme les routes publiques
// Exemple : app.use('/api/public', publicRoutes);

module.exports = app; // Exporter l'app pour l'utiliser dans server.js
