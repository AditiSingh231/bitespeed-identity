import express from 'express';
import bodyParser from 'body-parser';
import cors from 'cors';
import identifyRouter from './routes/identify';

const app = express();
app.use(cors());
app.use(bodyParser.json());

app.use('/identify', identifyRouter);

app.get('/', (_req, res) => {
  res.json({ status: 'ok', message: 'Bitespeed identity service. POST to /identify' });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
