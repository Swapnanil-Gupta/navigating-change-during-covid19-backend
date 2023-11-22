const { Router } = require("express");
const { query } = require("../lib/db");
const {
  emissionData,
  state,
  fuelType,
  energySector,
  covidData,
} = require("../lib/table");

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

  const includedSectors = req.query.includedSectors || [];
  const includedSectorsPlaceholder = includedSectors
    .map((_, i) => `:sector${i}`)
    .join(",");
  const includedSectorsBind = {};
  includedSectors.map((v, i) => (includedSectorsBind[`sector${i}`] = v));

  try {
    const [allCovidData, allSectors, allEmissionsData] = await Promise.all([
      query(
        `
          SELECT year, sum(count_confirmed_cases) as count_cases
          FROM ${covidData}
          WHERE state_code = :stateCode
          AND year >= :startYear
          AND year <= :endYear
          GROUP BY year, state_code
        `,
        { stateCode, startYear, endYear }
      ),
      query(
        `
          SELECT UNIQUE sector_code, sector_name
          FROM ${energySector}
          ${
            includedSectors.length != 0
              ? `WHERE sector_code IN (${includedSectorsPlaceholder})`
              : ""
          }
        `,
        includedSectorsBind
      ),
      query(
        `WITH avg_sector_emission(year,sector_code,state_code,avg_emission) AS (
          SELECT emis.year, emis.sector_code, emis.state_code, AVG(emis.emission) as avg_emission
          FROM ${emissionData} emis
          INNER JOIN ${state} s ON s.state_code = emis.state_code
          INNER JOIN ${energySector} sect ON emis.sector_code = sect.sector_code
          INNER JOIN ${fuelType} ft ON ft.fuel_type_code = emis.fuel_type_code
          WHERE emis.emission IS NOT NULL
          GROUP BY emis.year, emis.sector_code, emis.state_code
          ORDER BY emis.year ASC, emis.state_code ASC
          ),
          total_yearly_state_emission(year, state_code, total_emission) AS (
          SELECT emis.year, emis.state_code, SUM(emis.emission) as total_emis_per_year
          FROM ${emissionData} emis
          INNER JOIN ${state} s ON s.state_code = emis.state_code
          INNER JOIN ${energySector} sect ON emis.sector_code = sect.sector_code
          INNER JOIN ${fuelType} ft ON ft.fuel_type_code = emis.fuel_type_code
          WHERE emis.emission IS NOT NULL
          AND ft.fuel_type_code != 400
          GROUP BY emis.year, emis.state_code
          ORDER BY emis.year ASC, emis.state_code ASC
          ),
          total_year_sector_emission(year, state_code, sector_code, total_emission) AS (
          SELECT emis.year, emis.state_code, sect.sector_code, SUM(emis.emission) as total_emis_per_year
          FROM ${emissionData} emis
          INNER JOIN ${state} s ON s.state_code = emis.state_code
          INNER JOIN ${energySector} sect ON emis.sector_code = sect.sector_code
          INNER JOIN ${fuelType} ft ON ft.fuel_type_code = emis.fuel_type_code
          WHERE emis.emission IS NOT NULL
          AND ft.fuel_type_code != 400
          GROUP BY emis.year, emis.state_code, sect.sector_code
          ORDER BY emis.year ASC, emis.state_code ASC
          )
        SELECT avgE.year, s.state_name, sect.sector_name, avgE.avg_emission AS avg_sector_emission, (tyse.total_emission/tye.total_emission)*100 AS percent_of_total_emission
        FROM avg_sector_emission avgE
        INNER JOIN total_yearly_state_emission tye ON avgE.year=tye.year AND avgE.state_code=tye.state_code
        INNER JOIN total_year_sector_emission tyse ON avgE.year=tyse.year AND avgE.state_code=tyse.state_code AND avgE.sector_code = tyse.sector_code
        INNER JOIN ${state} s ON s.state_code=avgE.state_code
        INNER JOIN ${energySector} sect ON sect.sector_code=avgE.sector_code
        WHERE s.state_code = :stateCode 
        AND avgE.year >= :startYear
        AND avgE.year <= :endYear
        ${
          includedSectors.length != 0
            ? `AND sect.sector_code IN (${includedSectorsPlaceholder})`
            : ""
        }
        ORDER BY avgE.year ASC, avgE.state_code ASC, avgE.sector_code ASC
      `,
        includedSectors.length == 0
          ? { stateCode, startYear, endYear }
          : { stateCode, startYear, endYear, ...includedSectorsBind }
      ),
    ]);

    if (allEmissionsData.length == 0) {
      return res.status(404).json({ error: "No data found for the state" });
    }

    const allSectorNames = allSectors.map((i) => i.sectorName);

    const covidDataMap = new Map();
    for (let row of allCovidData) {
      const { year, countCases } = row;
      covidDataMap.set(year, countCases);
    }

    const emissionsDataMap = new Map();
    for (let row of allEmissionsData) {
      const { year, sectorName, percentOfTotalEmission } = row;
      if (emissionsDataMap.has(year)) {
        emissionsDataMap.get(year)[sectorName] = percentOfTotalEmission;
      } else {
        let obj = {};
        for (let name of allSectorNames) {
          obj[name] = 0;
        }
        obj[sectorName] = percentOfTotalEmission;
        emissionsDataMap.set(year, obj);
      }
    }

    let payload = [["Year", "Confirmed COVID-19 Cases", ...allSectorNames]];
    for (let year of emissionsDataMap.keys()) {
      payload.push([
        year,
        covidDataMap.has(year) ? covidDataMap.get(year) : 0,
        ...Object.values(emissionsDataMap.get(year)),
      ]);
    }
    res.json(payload);
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
        SELECT sect.sector_name, SUM(avgEmission.avg_emission) as total_avg_emission
        FROM avgEmission
        INNER JOIN ${state} s ON s.state_code=avgEmission.state_code
        INNER JOIN ${energySector} sect ON sect.sector_code=avgEmission.sector_code
        WHERE s.state_code = :stateCode 
        AND avgEmission.year >= :startYear
        AND avgEmission.year <= :endYear
        GROUP BY sect.sector_name
        ORDER BY total_avg_emission DESC
      `,
      { stateCode, startYear, endYear }
    );

    if (data.length == 0) {
      return res.status(404).json({ error: "No data found for the state" });
    }

    const map = new Map();
    for (let row of data) {
      const { sectorName, totalAvgEmission } = row;
      map.set(sectorName, totalAvgEmission);
    }

    const payload = [["Energy Sector", "Total Average Emissions (PPM)"]];
    for (let key of map.keys()) {
      payload.push([key, map.get(key)]);
    }

    res.json(payload);
  } catch (err) {
    console.error("Failed to fetch emission top-5 bar graph data");
    console.error(err);
    res.status(500).json({ error: "Failed to fetch top-5 bar graph data" });
  }
});

module.exports = emissionDataRouter;
