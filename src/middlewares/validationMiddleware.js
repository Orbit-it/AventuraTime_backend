const { body, validationResult } = require('express-validator');

// Validation pour mettre à jour un article
exports.validateUpdateItem = [
  body('id').notEmpty().withMessage('ID is required'),
  body('nouvelle_qté').isInt({ min: 0 }).withMessage('Quantity must be a positive number'),
  body('code_emplacement').optional().isString().withMessage('Code emplacement must be a string'),
  (req, res, next) => {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }
    next();
  },
];
