const { Router } = require("express");
const { query } = require("../lib/db");
const {
  businessData,
  industry,
  businessSize,
  covidData,
  state,
} = require("../lib/table");

const entrepreneurshipDataRouter = Router();

entrepreneurshipDataRouter.get("/", async (req, res) => {
  const stateCode = parseInt(req.query.stateCode);
  if (!stateCode) return res.status(400).json({ error: "Invalid state code" });

  const industryCode = parseInt(req.query.industryCode);
  if (!industryCode)
    return res.status(400).json({ error: "Invalid industry code" });

  const startYear = parseInt(req.query.startYear) || 2010;
  const endYear = parseInt(req.query.endYear) || 2021;

  const includedSizes = req.query.includedSizes || [];
  const includedSizesPlaceholder = includedSizes
    .map((_, i) => `:size${i}`)
    .join(",");
  const includedSizesBind = {};
  includedSizes.map((v, i) => (includedSizesBind[`size${i}`] = v));

  try {
    const [allCovidData, allSizes, allEntrepreneurshipData] = await Promise.all(
      [
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
          SELECT UNIQUE size_code, size_name
          FROM ${businessSize}
          ${
            includedSizes.length != 0
              ? `WHERE size_code IN (${includedSizesPlaceholder})`
              : ""
          }
        `,
          includedSizesBind
        ),
        query(
          `
          WITH TotalEstablishments AS (
            SELECT 
              year, 
              state_code, 
              industry_code, 
              size_code,
              SUM(count_establishments) as total_establishments
            FROM ${businessData}
            WHERE count_establishments IS NOT NULL
            GROUP BY year, state_code, industry_code, size_code
          )
          SELECT
            curr.year,
            bs.size_name,
            (curr.total_establishments - prev.total_establishments) as change
          FROM TotalEstablishments curr
          INNER JOIN TotalEstablishments prev
          ON curr.year = prev.year + 1
          AND curr.state_code = prev.state_code
          AND curr.industry_code = prev.industry_code
          AND curr.size_code = prev.size_code
          INNER JOIN ${businessSize} bs ON bs.size_code = curr.size_code
          WHERE curr.state_code = :stateCode
          AND curr.industry_code = :industryCode 
          AND curr.year >= :startYear
          AND curr.year <= :endYear
          ${
            includedSizes.length != 0
              ? `AND curr.size_code IN (${includedSizesPlaceholder})`
              : ""
          }
          ORDER BY curr.year ASC, curr.state_code ASC, bs.size_name ASC
        `,
          includedSizes.length == 0
            ? { stateCode, industryCode, startYear, endYear }
            : {
                stateCode,
                industryCode,
                startYear,
                endYear,
                ...includedSizesBind,
              }
        ),
      ]
    );

    if (allEntrepreneurshipData.length == 0) {
      return res.status(404).json({ error: "No data found for the state" });
    }

    const allSizeNames = allSizes.map((i) => i.sizeName);

    const covidDataMap = new Map();
    for (let row of allCovidData) {
      const { year, countCases } = row;
      covidDataMap.set(year, countCases);
    }

    const entrepreneurshipDataMap = new Map();
    const startYearObj = {};
    for (let name of allSizeNames) {
      startYearObj[name] = 0;
    }
    entrepreneurshipDataMap.set(startYear, startYearObj);

    for (let row of allEntrepreneurshipData) {
      const { year, sizeName, change } = row;
      if (entrepreneurshipDataMap.has(year)) {
        entrepreneurshipDataMap.get(year)[sizeName] = change;
      } else {
        let obj = {};
        for (let name of allSizeNames) {
          obj[name] = 0;
        }
        obj[sizeName] = change;
        entrepreneurshipDataMap.set(year, obj);
      }
    }

    let payload = [["Year", "Confirmed COVID-19 Cases", ...allSizeNames]];
    for (let year of entrepreneurshipDataMap.keys()) {
      payload.push([
        year,
        covidDataMap.has(year) ? covidDataMap.get(year) : 0,
        ...Object.values(entrepreneurshipDataMap.get(year)),
      ]);
    }
    res.json(payload);
  } catch (err) {
    console.error("Failed to fetch entrepreneurship data");
    console.error(err);
    res.status(500).json({ error: "Failed to fetch entrepreneurship data" });
  }
});

module.exports = entrepreneurshipDataRouter;
