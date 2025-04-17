const xlsx = require('xlsx');

// Fonction pour lire un fichier Excel et insérer les articles dans la base de données
const importItemsFromExcel = (filePath, clientDb) => {
  const workbook = xlsx.readFile(filePath);
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const data = xlsx.utils.sheet_to_json(sheet);

  data.forEach(async (item) => {
    const { name, quantity, barcode } = item;
    try {
      await clientDb.query(
        'INSERT INTO items (name, quantity, barcode) VALUES ($1, $2, $3)',
        [name, quantity, barcode]
      );
    } catch (err) {
      console.error('Error importing item', err);
    }
  });
};

module.exports = importItemsFromExcel;


