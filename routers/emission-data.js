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
          AND count_confirmed_cases IS NOT NULL
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
        `WITH total_yearly_state_emission AS (
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
        total_year_sector_emission AS (
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
        SELECT 
          tye.year, 
          sect.sector_name, 
          (tyse.total_emis_per_year/tye.total_emis_per_year)*100 AS percent_of_total_emission
        FROM total_yearly_state_emission tye
        INNER JOIN total_year_sector_emission tyse ON tye.year=tyse.year AND tye.state_code=tyse.state_code
        INNER JOIN ${energySector} sect ON sect.sector_code=tyse.sector_code
        WHERE tye.state_code = :stateCode 
        AND tye.year >= :startYear
        AND tye.year <= :endYear
        ${
          includedSectors.length != 0
            ? `AND sect.sector_code IN (${includedSectorsPlaceholder})`
            : ""
        }
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

emissionDataRouter.get("/top-sectors", async (req, res) => {
  const stateCode = parseInt(req.query.stateCode);
  if (!stateCode || Number.isNaN(stateCode))
    return res.status(400).json({ error: "Invalid state code" });

  const startYear = parseInt(req.query.startYear) || 1970;
  const endYear = parseInt(req.query.endYear) || 2021;

  try {
    const data = await query(
      `
        SELECT e.sector_name, ROUND(SUM(emis.emission), 2) as total_emis
        FROM ${emissionData} emis
        INNER JOIN ${energySector} e ON e.sector_code = emis.sector_code
        WHERE emis.emission IS NOT NULL
        AND emis.fuel_type_code != 400
        AND emis.state_code = :stateCode
        AND emis.year >= :startYear
        AND emis.year <= :endYear
        GROUP BY e.sector_name
        ORDER BY total_emis DESC
      `,
      { stateCode, startYear, endYear }
    );

    if (data.length == 0) {
      return res.status(404).json({ error: "No data found for the state" });
    }

    const map = new Map();
    for (let row of data) {
      const { sectorName, totalEmis } = row;
      map.set(sectorName, totalEmis);
    }

    const payload = [["Energy Sector", "Total Fossil Fuel Emissions (PPM)"]];
    for (let key of map.keys()) {
      payload.push([key, map.get(key)]);
    }

    res.json(payload);
  } catch (err) {
    console.error("Failed to fetch top energy sectors");
    console.error(err);
    res.status(500).json({ error: "Failed to fetch top energy sectors" });
  }
});

emissionDataRouter.get("/geo", async (req, res) => {
  const startYear = parseInt(req.query.startYear) || 2010;
  const endYear = parseInt(req.query.endYear) || 2021;

  try {
    const geoData = await query(
      `
        SELECT s.state_name, ROUND(SUM(emis.emission), 2) as total_emis
        FROM ${emissionData} emis
        INNER JOIN ${state} s ON s.state_code = emis.state_code
        WHERE emis.emission IS NOT NULL
        AND emis.fuel_type_code != 400
        AND emis.year >= :startYear
        AND emis.year <= :endYear
        GROUP BY emis.state_code, s.state_name
        ORDER BY total_emis DESC
      `,
      { startYear, endYear }
    );

    if (geoData.length == 0) {
      return res.status(404).json({ error: "No data found for the state" });
    }

    const map = new Map();
    for (let row of geoData) {
      const { stateName, totalEmis } = row;
      map.set(stateName, totalEmis);
    }

    const payload = [["State", "Total Fossil Fuel Emissions (PPM)"]];
    for (let key of map.keys()) {
      payload.push([key, map.get(key)]);
    }

    res.json(payload);
  } catch (err) {
    console.error("Failed to fetch geo data");
    console.error(err);
    res.status(500).json({ error: "Failed to fetch geo data" });
  }
});

module.exports = emissionDataRouter;
