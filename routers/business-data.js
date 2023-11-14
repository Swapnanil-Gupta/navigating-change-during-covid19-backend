const { Router } = require("express");
const { query } = require("../lib/db");
const {
  businessData,
  state,
  businessSize,
  businessType,
  industry,
} = require("../lib/table");

const businessDataRouter = Router();

businessDataRouter.get("/industry", async (req, res) => {
  try {
    const data = await query(`
      SELECT UNIQUE industry_code, industry_name
      FROM ${industry}
      ORDER BY industry_code ASC
    `);
    res.json(data);
  } catch (err) {
    console.error("Failed to fetch industries");
    console.error(err);
    res.status(500).json({ error: "Failed to fetch industries" });
  }
});

businessDataRouter.get("/", async (req, res) => {
  const stateCode = parseInt(req.query.stateCode);
  if (!stateCode || Number.isNaN(stateCode))
    return res.status(400).json({ error: "Invalid state code" });

  const startYear = parseInt(req.query.startYear) || 2012;
  const endYear = parseInt(req.query.endYear) || 2021;

  try {
    const data = await query(
      `
      SELECT 
        bd.year, 
        s.state_name, 
        i.industry_name, 
        sum(bd.count_establishments) AS count_establishments
      FROM ${businessData} bd
      INNER JOIN ${state} s ON s.state_code = bd.state_code
      INNER JOIN ${businessType} bt ON bd.type_code = bt.type_code
      INNER JOIN ${businessSize} bs ON bs.size_code = bd.size_code
      INNER JOIN ${industry} i ON i.industry_code = bd.industry_code
      WHERE s.state_code = :stateCode
      AND bd.year >= :startYear
      AND bd.year <= :endYear
      AND bd.count_establishments IS NOT NULL
      GROUP BY bd.year, s.state_name, i.industry_name
      ORDER BY bd.year ASC, s.state_name ASC
    `,
      { stateCode, startYear, endYear }
    );

    const map = new Map();
    for (let row of data) {
      if (map.has(row.year)) {
        map.get(row.year)[row.industryName] = row.countEstablishments;
      } else {
        map.set(row.year, {
          year: row.year,
          [row.industryName]: row.countEstablishments,
        });
      }
    }

    res.json(Array.from(map.values()));
  } catch (err) {
    console.error("Failed to fetch business data");
    console.error(err);
    res.status(500).json({ error: "Failed to fetch business data" });
  }
});

module.exports = businessDataRouter;
