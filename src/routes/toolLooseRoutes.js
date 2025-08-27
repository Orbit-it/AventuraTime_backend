const express = require("express");
const { addToolLoose, updateToolLoose, deleteToolLoose, getToolLooses, getToolLoosesByEmployee, getUnpaidToolLooses } = require("../controllers/toolLooseController");

const router = express.Router();

router.post("/tool-losses", addToolLoose); // Ajouter une perte d'outillage
router.get("/tool-losses", getToolLooses); // Liste des mises à 
router.put("/tool-losses/:id", updateToolLoose); // Mettre à jour une mise à pied
router.get("/tool-losses/:id", getToolLoosesByEmployee); // Liste des mises à pied par employé
router.delete("/tool-losses/:id", deleteToolLoose); // Supprimer une mise à pied

module.exports = router;