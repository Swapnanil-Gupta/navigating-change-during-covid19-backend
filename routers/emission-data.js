const { Router } = require("express");
const { query } = require("../lib/db");
const { emissionData, state, fuelType, energySector } = require("../lib/table");

const emissionDataRouter = Router();

// Query Sector Names and Codes
// http://localhost:3000/emission-data/energy-sector
emissionDataRouter.get("/energy-sector", async (req, res) => {
  try {
    const data = await query(`
      SELECT UNIQUE sector_code, sector_name
      FROM ${energySector}
      ORDER BY sector_code ASC
    `);
    res.json(data);
  } catch (err) {
    console.error("Failed to fetch energy sectors");
    console.error(err);
    res.status(500).json({ error: "Failed to fetch energy sectors" });
  }
});


// Query Data for line graph trends
// localhost:3000/emission-data?stateCode=12&startYear=1970&endYear=2021
emissionDataRouter.get("/", async (req, res) => {
  const stateCode = parseInt(req.query.stateCode);
  if (!stateCode || Number.isNaN(stateCode))
    return res.status(400).json({ error: "Invalid state code" });

    const startYear = parseInt(req.query.startYear) || 1970;
    const endYear = parseInt(req.query.endYear) || 2021;

  try {
    const data = await query(
      `
        WITH avgEmission(year,sector_code,state_code,avg_emission) AS (
            SELECT emis.year, emis.sector_code, emis.state_code, AVG(emis.emission) as avg_emission
            FROM ${emissionData} emis
            INNER JOIN ${state} s ON s.state_code = emis.state_code
            INNER JOIN ${energySector} sect ON emis.sector_code = sect.sector_code
            INNER JOIN ${fuelType} ft ON ft.fuel_type_code = emis.fuel_type_code
            WHERE emis.emission IS NOT NULL
            GROUP BY emis.year, emis.sector_code, emis.state_code
            ORDER BY emis.year ASC, emis.state_code ASC
        )
        SELECT avgEmission.year, s.state_name, sect.sector_name, avgEmission.avg_emission
        FROM avgEmission
        INNER JOIN ${state} s ON s.state_code=avgEmission.state_code
        INNER JOIN ${energySector} sect ON sect.sector_code=avgEmission.sector_code
        WHERE s.state_code = :stateCode 
        AND avgEmission.year >= :startYear
        AND avgEmission.year <= :endYear
        ORDER BY avgEmission.year ASC, s.state_name ASC
    `,
    { stateCode, startYear, endYear }
    );

    res.json(data);
  } catch (err) {
    console.error("Failed to fetch emission data");
    console.error(err);
    res.status(500).json({ error: "Failed to fetch emission data" });
  }
});

// Query data for top-5-sector bar graph
// localhost:3000/emission-data/top-5-sectors?stateCode=12&startYear=1970&endYear=2021
emissionDataRouter.get("/top-5-sectors", async (req, res) => {
  const stateCode = parseInt(req.query.stateCode);
  if (!stateCode || Number.isNaN(stateCode))
    return res.status(400).json({ error: "Invalid state code" });

    const startYear = parseInt(req.query.startYear) || 1970;
    const endYear = parseInt(req.query.endYear) || 2021;

  try {
    const data = await query(
      
      `
      WITH avgEmission(year,sector_code,state_code,avg_emission) AS (
        SELECT emis.year, emis.sector_code, emis.state_code, AVG(emis.emission) as avg_emission
        FROM ${emissionData} emis
        INNER JOIN ${state} s ON s.state_code = emis.state_code
        INNER JOIN ${energySector} sect ON emis.sector_code = sect.sector_code
        INNER JOIN ${fuelType} ft ON ft.fuel_type_code = emis.fuel_type_code
        WHERE emis.emission IS NOT NULL
        GROUP BY emis.year, emis.sector_code, emis.state_code
        ORDER BY emis.year ASC, emis.state_code ASC
      )
      SELECT avgEmission.year, s.state_name, sect.sector_name, MAX(avgEmission.avg_emission) as max_avg_emission
      FROM avgEmission
      INNER JOIN ${state} s ON s.state_code=avgEmission.state_code
      INNER JOIN ${energySector} sect ON sect.sector_code=avgEmission.sector_code
      WHERE s.state_code = :stateCode 
      AND avgEmission.year >= :startYear
      AND avgEmission.year <= :endYear
      GROUP BY avgEmission.year, s.state_name, sect.sector_name
      ORDER BY avgEmission.year ASC, s.state_name ASC
      `,
    { stateCode, startYear, endYear }
    );

    res.json(data);
  } catch (err) {
    console.error("Failed to fetch emission top-5 bar graph data");
    console.error(err);
    res.status(500).json({ error: "Failed to fetch top-5 bar graph data" });
  }
});

module.exports = emissionDataRouter;
