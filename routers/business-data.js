const { Router } = require("express");
const { query } = require("../lib/db");
const { businessData, industry, covidData } = require("../lib/table");

const businessDataRouter = Router();

businessDataRouter.get("/industry", async (req, res) => {
  try {
    const data = await query(`
      SELECT UNIQUE industry_code, industry_name
      FROM ${industry}
      ORDER BY industry_name ASC
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
  if (!stateCode) return res.status(400).json({ error: "Invalid state code" });

  const startYear = parseInt(req.query.startYear) || 2010;
  const endYear = parseInt(req.query.endYear) || 2021;

  const excludedIndustries = req.query.excludedIndustries || [];
  const excludedIndustriesPlaceholder = excludedIndustries
    .map((_, i) => `:industry${i}`)
    .join(",");
  const excludedIndustriesBind = {};
  excludedIndustries.map(
    (v, i) => (excludedIndustriesBind[`industry${i}`] = v)
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
          GROUP BY year, state_code
        `,
        { stateCode, startYear, endYear }
      ),
      query(
        `
          SELECT UNIQUE industry_code, industry_name
          FROM ${industry}
          ${
            excludedIndustries.length != 0
              ? `WHERE industry_code NOT IN (${excludedIndustriesPlaceholder})`
              : ""
          }
        `,
        excludedIndustriesBind
      ),
      query(
        `
          WITH TotalEstablishments AS (
            SELECT 
                year, 
                state_code,
                sum(count_establishments) as total_establishments
            FROM ${businessData}
            WHERE state_code = :stateCode
            AND year >= :startYear
            AND year <= :endYear
            AND count_establishments IS NOT NULL
            GROUP BY year, state_code
          ), IndustryEstablishments AS (
            SELECT 
                bd.year,
                bd.state_code,
                i.industry_name,
                sum(bd.count_establishments) as industry_establishments
            FROM ${businessData} bd
            INNER JOIN ${industry} i on i.industry_code = bd.industry_code
            WHERE bd.state_code = :stateCode
            AND bd.year >= :startYear
            AND bd.year <= :endYear
            AND bd.count_establishments IS NOT NULL
            ${
              excludedIndustries.length != 0
                ? `AND bd.industry_code NOT IN (${excludedIndustriesPlaceholder})`
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
          INNER JOIN 
              IndustryEstablishments ie 
              ON ie.year = te.year
              AND ie.state_code = te.state_code
          ORDER BY ie.year ASC, ie.state_code ASC, ie.industry_name ASC
        `,
        excludedIndustries.length == 0
          ? { stateCode, startYear, endYear }
          : { stateCode, startYear, endYear, ...excludedIndustriesBind }
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

businessDataRouter.get("/top-5-industries", async (req, res) => {
  const stateCode = parseInt(req.query.stateCode);
  if (!stateCode || Number.isNaN(stateCode))
    return res.status(400).json({ error: "Invalid state code" });

  const startYear = parseInt(req.query.startYear) || 2012;
  const endYear = parseInt(req.query.endYear) || 2021;

  const topIndustriesData = await query(
    `
      WITH MaxIndusrty AS (
        SELECT 
            industry_code, 
            SUM(count_establishments) as count_establishments
        FROM ${businessData}
        WHERE state_code = :stateCode
        AND year >= :startYear
        AND year <= :endYear
        GROUP BY industry_code
        ORDER BY count_establishments DESC
        FETCH FIRST 5 ROWS ONLY
      )
      SELECT 
        m.industry_code, i.industry_name, m.count_establishments
      FROM MaxIndusrty m
      INNER JOIN ${industry} i on i.industry_code = m.industry_code
    `,
    { stateCode, startYear, endYear }
  );

  if (topIndustriesData.length == 0) {
    return res.status(404).json({ error: "No data found for the state" });
  }

  const map = new Map();
  for (let row of topIndustriesData) {
    const { industryName, countEstablishments } = row;
    map.set(industryName, countEstablishments);
  }

  const payload = [["Industry Name", "Count of Establishments"]];
  for (let key of map.keys()) {
    payload.push([key, map.get(key)]);
  }

  res.json(payload);
});

module.exports = businessDataRouter;
