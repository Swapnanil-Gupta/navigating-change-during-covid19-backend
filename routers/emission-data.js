const { Router } = require("express");
const { query } = require("../lib/db");
const {
  emissionData,
  state,
  fuelType,
  sector,
} = require("../lib/table");

const emissionDataRouter = Router();

emissionDataRouter.get("/sector", async (req, res) => {
  try {
    const data = await query(`
      SELECT UNIQUE sector_code, sector_name
      FROM ${sector}
      ORDER BY sector_code ASC
    `);
    res.json(data);
  } catch (err) {
    console.error("Failed to fetch sectors");
    console.error(err);
    res.status(500).json({ error: "Failed to fetch sectors" });
  }
});

emissionDataRouter.get("/", async (req, res) => {
  const stateCode = parseInt(req.query.stateCode);
  if (!stateCode || Number.isNaN(stateCode))
    return res.status(400).json({ error: "Invalid state code" });

  try {
    const data = await query(`
        WITH avgEmission(year,sector_code,state_code,avg_emission) AS (
            SELECT emis.year, emis.sector_code, emis.state_code, AVG(emis.emission) as avg_emission
            FROM ${emissionData} emis
            INNER JOIN ${state} s ON s.state_code = emis.state_code
            INNER JOIN ${sector} sect ON emis.sector_code = sect.sector_code
            INNER JOIN ${fuelType} ft ON ft.fuel_type_code = emis.fuel_type_code
            WHERE emis.emission IS NOT NULL
            GROUP BY emis.year, emis.sector_code, emis.state_code
            ORDER BY emis.year ASC, emis.state_code ASC
        )
        SELECT avgEmission.year, s.state_name, sect.sector_name, avgEmission.avg_emission
        FROM avgEmission
        INNER JOIN ${state} s ON s.state_code=avgEmission.state_code
        INNER JOIN ${sector} sect ON sect.sector_code=avgEmission.sector_code
        ORDER BY avgEmission.year ASC, s.state_name ASC
    
    `
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
    console.error("Failed to fetch emission data");
    console.error(err);
    res.status(500).json({ error: "Failed to fetch emission data" });
  }
});

module.exports = emissionDataRouter;
