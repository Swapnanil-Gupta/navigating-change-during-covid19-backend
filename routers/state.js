const { Router } = require("express");
const { query } = require("../lib/db");
const { state } = require("../lib/table");

const stateRouter = Router();

stateRouter.get("/", async (req, res) => {
  try {
    const data = await query(`
      SELECT UNIQUE state_code, state_name
      FROM ${state}
      ORDER BY state_code ASC
    `);
    res.json(data);
  } catch (err) {
    console.error("Failed to fetch states");
    console.error(err);
    res.status(500).json({ error: "Failed to fetch states" });
  }
});

module.exports = stateRouter;
