// server.js
const app = require('./src/app'); // Importer l'application Express depuis app.js
require('./src/services/scheduler'); // Auto-runs attendance job
const http = require('http');
const dotenv = require('dotenv');
const { Client } = require('pg');



// Charger les variables d'environnement
dotenv.config();

// Récupérer le port depuis les variables d'environnement ou utiliser 3000 par défaut
const port = process.env.PORT || 3000;

// Créer un serveur HTTP avec l'application Express
const server = http.createServer(app);




const pgClient = new Client({
  user: process.env.DB_ADMIN_USER,
  host: process.env.DB_ADMIN_HOST,
  database: process.env.DB_ADMIN_NAME,
  password: process.env.DB_ADMIN_PASSWORD,
  port: process.env.DB_ADMIN_PORT,
});

// Se connecter à PostgreSQL avec pg
pgClient.connect()
  .then(() => {
    console.log("✅ Connexion à la base de données PostgreSQL réussie !");

    /* Commencer à écouter les notifications
    pgClient.query("LISTEN inventory_update");
    pgClient.on("notification", async (msg) => {
      console.log("🔔 Notification reçue :", msg.payload);
      // Lorsque la notification est reçue, on récupère l'inventaire mis à jour
      const updatedInventory = await Inventory.findAll();
      io.emit("inventoryUpdate", updatedInventory);
    }); */

  }) 
  .catch((err) => {
    console.error("❌ Erreur de connexion à PostgreSQL :", err);
  });

/* Connexion WebSocket
io.on("connection", async (socket) => {
  console.log("✅ Client connecté:", socket.id);

  try {
    // Envoyer l'état initial de l'inventaire
    const inventory = await Inventory.findAll();
    socket.emit("inventoryUpdate", inventory);
  } catch (err) {
    console.error("⚠️ Erreur chargement inventaire :", err);
  }

  socket.on("disconnect", () => {
    console.log("❌ Client déconnecté:", socket.id);
  });
});  */



// Démarrer le serveur
server.listen(port,'0.0.0.0', () => {
  console.log(`Server running on port ${port}`);

});
