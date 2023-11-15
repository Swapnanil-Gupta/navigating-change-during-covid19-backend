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
    const allIndustries = await query(`
      SELECT UNIQUE industry_code, industry_name
      FROM ${industry}
      ORDER BY industry_code ASC
    `);
    const allIndustryNames = allIndustries.map((i) => i.industryName);
    const allBusinessData = await query(
      `
        SELECT 
          bd.year,
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

    if (allBusinessData.length == 0) {
      return res.status(404).json({ error: "No data found for the state" });
    }

    const map = new Map();
    for (let row of allBusinessData) {
      const { year, industryName, countEstablishments } = row;
      if (map.has(year)) {
        map.get(year)[industryName] = countEstablishments;
      } else {
        let obj = {};
        for (let name of allIndustryNames) {
          obj[name] = 0;
        }
        obj[industryName] = countEstablishments;
        map.set(year, obj);
      }
    }

    let payload = [["Year", ...allIndustryNames]];
    for (let key of map.keys()) {
      payload.push([key, ...Object.values(map.get(key))]);
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
        FROM Business_Data
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
      INNER JOIN Industry i on i.industry_code = m.industry_code
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
