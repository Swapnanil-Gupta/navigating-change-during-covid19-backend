const { Router } = require("express");
const { query } = require("../lib/db");
const { businessData, industry, covidData, state } = require("../lib/table");

const businessDataRouter = Router();

businessDataRouter.get("/", async (req, res) => {
  const stateCode = parseInt(req.query.stateCode);
  if (!stateCode) return res.status(400).json({ error: "Invalid state code" });

  const startYear = parseInt(req.query.startYear) || 2010;
  const endYear = parseInt(req.query.endYear) || 2021;

  const includedIndustries = req.query.includedIndustries || [];
  const includedIndustriesPlaceholder = includedIndustries
    .map((_, i) => `:industry${i}`)
    .join(",");
  const includedIndustriesBind = {};
  includedIndustries.map(
    (v, i) => (includedIndustriesBind[`industry${i}`] = v)
  );

  try {
    const [allCovidData, allIndustries, allBusinessData] = await Promise.all([
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
          SELECT UNIQUE industry_code, industry_name
          FROM ${industry}
          ${
            includedIndustries.length != 0
              ? `WHERE industry_code IN (${includedIndustriesPlaceholder})`
              : ""
          }
        `,
        includedIndustriesBind
      ),
      query(
        `
          WITH TotalEstablishments AS (
            SELECT 
                year, 
                state_code,
                sum(count_establishments) as total_establishments
            FROM ${businessData}
            WHERE count_establishments IS NOT NULL
            GROUP BY year, state_code
          ), IndustryEstablishments AS (
            SELECT 
                bd.year,
                bd.state_code,
                i.industry_name,
                sum(bd.count_establishments) as industry_establishments
            FROM ${businessData} bd
            INNER JOIN ${industry} i on i.industry_code = bd.industry_code
            WHERE bd.count_establishments IS NOT NULL
            ${
              includedIndustries.length != 0
                ? `AND bd.industry_code IN (${includedIndustriesPlaceholder})`
                : ""
            }
            GROUP BY bd.year, bd.state_code, i.industry_name
          )
          SELECT
              ie.year,
              ie.state_code,
              ie.industry_name,
              ROUND((ie.industry_establishments / te.total_establishments) * 100, 2) as percent_establishments
          FROM TotalEstablishments te
          INNER JOIN IndustryEstablishments ie ON ie.year = te.year
          AND ie.state_code = te.state_code
          WHERE ie.state_code = :stateCode
          AND ie.year >= :startYear
          AND ie.year <= :endYear
          ORDER BY ie.year ASC, ie.state_code ASC, ie.industry_name ASC
        `,
        includedIndustries.length == 0
          ? { stateCode, startYear, endYear }
          : { stateCode, startYear, endYear, ...includedIndustriesBind }
      ),
    ]);

    if (allBusinessData.length == 0) {
      return res.status(404).json({ error: "No data found for the state" });
    }

    const allIndustryNames = allIndustries.map((i) => i.industryName);

    const covidDataMap = new Map();
    for (let row of allCovidData) {
      const { year, countCases } = row;
      covidDataMap.set(year, countCases);
    }

    const businessDataMap = new Map();
    for (let row of allBusinessData) {
      const { year, industryName, percentEstablishments } = row;
      if (businessDataMap.has(year)) {
        businessDataMap.get(year)[industryName] = percentEstablishments;
      } else {
        let obj = {};
        for (let name of allIndustryNames) {
          obj[name] = 0;
        }
        obj[industryName] = percentEstablishments;
        businessDataMap.set(year, obj);
      }
    }

    let payload = [["Year", "Confirmed COVID-19 Cases", ...allIndustryNames]];
    for (let year of businessDataMap.keys()) {
      payload.push([
        year,
        covidDataMap.has(year) ? covidDataMap.get(year) : 0,
        ...Object.values(businessDataMap.get(year)),
      ]);
    }
    res.json(payload);
  } catch (err) {
    console.error("Failed to fetch business data");
    console.error(err);
    res.status(500).json({ error: "Failed to fetch business data" });
  }
});

businessDataRouter.get("/top-industries", async (req, res) => {
  const stateCode = parseInt(req.query.stateCode);
  if (!stateCode || Number.isNaN(stateCode))
    return res.status(400).json({ error: "Invalid state code" });

  const startYear = parseInt(req.query.startYear) || 2010;
  const endYear = parseInt(req.query.endYear) || 2021;

  try {
    const topIndustriesData = await query(
      `
        WITH TotalEstablishments AS (
          SELECT 
            year,
            state_code,
            industry_code,
            SUM(count_establishments) as total_establishments
          FROM ${businessData}
          WHERE count_establishments IS NOT NULL
          GROUP BY year, state_code, industry_code
        )
        SELECT 
          i.industry_name,
          FLOOR(AVG(te.total_establishments)) as avg_establishments
        FROM TotalEstablishments te
        INNER JOIN ${industry} i on i.industry_code = te.industry_code
        WHERE state_code = :stateCode
        AND year >= :startYear
        AND year <= :endYear
        GROUP BY te.industry_code, i.industry_name
        ORDER BY avg_establishments DESC
      `,
      { stateCode, startYear, endYear }
    );

    if (topIndustriesData.length == 0) {
      return res.status(404).json({ error: "No data found for the state" });
    }

    const map = new Map();
    for (let row of topIndustriesData) {
      const { industryName, avgEstablishments } = row;
      map.set(industryName, avgEstablishments);
    }

    const payload = [
      ["Industry Name", "Average Count of Business Establishments"],
    ];
    for (let key of map.keys()) {
      payload.push([key, map.get(key)]);
    }

    res.json(payload);
  } catch (err) {
    console.error("Failed to fetch top industries");
    console.error(err);
    res.status(500).json({ error: "Failed to fetch top industries" });
  }
});

businessDataRouter.get("/geo", async (req, res) => {
  const startYear = parseInt(req.query.startYear) || 2010;
  const endYear = parseInt(req.query.endYear) || 2021;

  try {
    const geoData = await query(
      `
        WITH TotalEstablishments AS (
          SELECT 
              year, 
              state_code,
              sum(count_establishments) as total_establishments
          FROM ${businessData}
          WHERE count_establishments IS NOT NULL
          GROUP BY year, state_code
        )
        SELECT 
            s.state_name,
            FLOOR(AVG(te.total_establishments)) as avg_establishments
        FROM TotalEstablishments te
        INNER JOIN ${state} s ON s.state_code = te.state_code
        WHERE te.year >= :startYear
        AND te.year <= :endYear
        GROUP BY s.state_name
      `,
      { startYear, endYear }
    );

    if (geoData.length == 0) {
      return res.status(404).json({ error: "No data found for the state" });
    }

    const map = new Map();
    for (let row of geoData) {
      const { stateName, avgEstablishments } = row;
      map.set(stateName, avgEstablishments);
    }

    const payload = [["State", "Average Count of Business Establishments"]];
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

module.exports = businessDataRouter;
