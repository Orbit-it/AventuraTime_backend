const express = require("express");
const { addLayoff, getLayoffs, updateLayoff, getLayoffsByEmployee, deleteLayoff } = require("../controllers/layoffController");

const router = express.Router();

router.post("/layoffs", addLayoff); // Ajouter une mise à pied
router.get("/layoffs", getLayoffs); // Liste des mises à pied
router.put("/layoffs/:id", updateLayoff); // Mettre à jour une mise à pied
router.get("/layoffs/:id", getLayoffsByEmployee); // Liste des mises à pied par employé
router.delete("/layoffs/:id", deleteLayoff); // Supprimer une mise à pied

module.exports = router;