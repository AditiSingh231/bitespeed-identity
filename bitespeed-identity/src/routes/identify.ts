import express from 'express';
import { handleIdentifyRequest } from '../services/identifyService';

const router = express.Router();

router.post('/', (req, res) => {
  try {
    const { email, phoneNumber } = req.body;
    const result = handleIdentifyRequest({ email: email ?? null, phoneNumber: phoneNumber ?? null });
    res.status(200).json(result);
  } catch (err: any) {
    res.status(400).json({ error: err.message || 'bad request' });
  }
});

export default router;
