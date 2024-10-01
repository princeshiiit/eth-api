const express = require('express');
const { ethers } = require('ethers');
const redis = require('redis');
const mongoose = require('mongoose');
const { Pool } = require('pg');
require('dotenv').config();

const app = express();
app.use(express.json());

const redisClient = redis.createClient({ url: process.env.REDIS_URL });
redisClient.connect();

const provider = new ethers.JsonRpcProvider(process.env.INFURA_URL);

const useMongoDB = true;
let Account;

if (useMongoDB) {
  mongoose.connect(process.env.MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true });
  const accountSchema = new mongoose.Schema({
    address: String,
    balance: String,
    updatedAt: { type: Date, default: Date.now },
  });
  Account = mongoose.model('Account', accountSchema);
} else {
  const pool = new Pool({ connectionString: process.env.POSTGRES_URI });
  pool.query(`
    CREATE TABLE IF NOT EXISTS accounts (
      address TEXT PRIMARY KEY,
      balance TEXT,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
}

const getOrSetCache = async (key, cb) => {
  const cached = await redisClient.get(key);
  if (cached) return JSON.parse(cached);
  const freshData = await cb();
  await redisClient.set(key, JSON.stringify(freshData), { EX: 60 });
  return freshData;
};

app.get('/eth-info/:address', async (req, res) => {
  const address = req.params.address;

  if (!ethers.isAddress(address)) {
    return res.status(400).json({ error: 'Invalid Ethereum address' });
  }

  try {
    const feeData = await getOrSetCache('feeData', async () => provider.getFeeData());
    const blockNumber = await getOrSetCache('blockNumber', async () => provider.getBlockNumber());
    const balance = await provider.getBalance(address);

    if (useMongoDB) {
      await Account.findOneAndUpdate(
        { address },
        { balance: ethers.formatEther(balance), updatedAt: new Date() },
        { upsert: true, new: true }
      );
    } else {
      await pool.query(
        'INSERT INTO accounts (address, balance, updated_at) VALUES ($1, $2, NOW()) ON CONFLICT (address) DO UPDATE SET balance = $2, updated_at = NOW();',
        [address, ethers.formatEther(balance)]
      );
    }

    res.json({
      gasPrice: ethers.formatUnits(feeData.gasPrice, 'gwei'),
      blockNumber,
      balance: ethers.formatEther(balance),
    });
  } catch (error) {
    res.status(500).json({ error: 'Error fetching Ethereum data'});
  }
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
