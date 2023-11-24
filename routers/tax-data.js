const { Router } = require("express");
const { query } = require("../lib/db");
const { taxData, taxCategory, covidData, state } = require("../lib/table");

const taxDataRouter = Router();

taxDataRouter.get("/", async (req, res) => {
  const stateCode = parseInt(req.query.stateCode);
  if (!stateCode) return res.status(400).json({ error: "Invalid state code" });

  const startYear = parseInt(req.query.startYear) || 2009;
  const endYear = parseInt(req.query.endYear) || 2022;

  const includedCategories = req.query.includedCategories || [];
  const includedCategoriesPlaceholder = includedCategories
    .map((_, i) => `:category${i}`)
    .join(",");
  const includedCategoriesBind = {};
  includedCategories.map(
    (v, i) => (includedCategoriesBind[`category${i}`] = v)
  );

  try {
    const [allCovidData, allCategories, allTaxData] = await Promise.all([
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
          SELECT UNIQUE category_code, category_name
          FROM ${taxCategory}
          ${
            includedCategories.length != 0
              ? `WHERE category_code IN (${includedCategoriesPlaceholder})`
              : ""
          }
        `,
        includedCategoriesBind
      ),
      query(
        `
          WITH TotalAmount AS (
            SELECT
              year,
              state_code,
              SUM(amount) as total_amount
            FROM ${taxData}
            WHERE amount != 0 AND amount IS NOT NULL
            GROUP BY year, state_code
          ),
          PercentAmount AS (
            SELECT
              td.year,
              td.state_code,
              td.category_code,
              td.amount,
              ta.total_amount,
              (td.amount / ta.total_amount) * 100 as percent_amount
            FROM TotalAmount ta
            INNER JOIN ${taxData} td ON td.year = ta.year
            AND td.state_code = ta.state_code
            WHERE td.amount != 0 AND td.amount IS NOT NULL
          )
          SELECT
            curr.year,
            tc.category_name,
            ROUND((curr.percent_amount - prev.percent_amount), 3) as change
          FROM PercentAmount curr
          INNER JOIN PercentAmount prev ON curr.year = prev.year + 1
          AND curr.state_code = prev.state_code
          AND curr.category_code = prev.category_code
          INNER JOIN ${taxCategory} tc ON tc.category_code = curr.category_code
          WHERE curr.state_code = :stateCode
          AND prev.year >= :startYear
          AND prev.year <= :endYear
          ${
            includedCategories.length != 0
              ? `AND curr.category_code IN (${includedCategoriesPlaceholder})`
              : ""
          }
          ORDER BY curr.year ASC, curr.state_code ASC, tc.category_name ASC
        `,
        includedCategories.length == 0
          ? { stateCode, startYear, endYear }
          : { stateCode, startYear, endYear, ...includedCategoriesBind }
      ),
    ]);

    if (allTaxData.length == 0) {
      return res.status(404).json({ error: "No data found for the state" });
    }

    const allCategoryNames = allCategories.map((i) => i.categoryName);

    const covidDataMap = new Map();
    for (let row of allCovidData) {
      const { year, countCases } = row;
      covidDataMap.set(year, countCases);
    }

    const taxDataMap = new Map();
    const startYearObj = {};
    for (let name of allCategoryNames) {
      startYearObj[name] = 0;
    }
    taxDataMap.set(startYear, startYearObj);

    for (let row of allTaxData) {
      const { year, categoryName, change } = row;
      if (taxDataMap.has(year)) {
        taxDataMap.get(year)[categoryName] = change;
      } else {
        let obj = {};
        for (let name of allCategoryNames) {
          obj[name] = 0;
        }
        obj[categoryName] = change;
        taxDataMap.set(year, obj);
      }
    }

    let payload = [["Year", "Confirmed COVID-19 Cases", ...allCategoryNames]];
    for (let year of taxDataMap.keys()) {
      payload.push([
        year,
        covidDataMap.has(year) ? covidDataMap.get(year) : 0,
        ...Object.values(taxDataMap.get(year)),
      ]);
    }
    res.json(payload);
  } catch (err) {
    console.error("Failed to fetch tax data");
    console.error(err);
    res.status(500).json({ error: "Failed to fetch tax data" });
  }
});

taxDataRouter.get("/top-categories", async (req, res) => {
  const stateCode = parseInt(req.query.stateCode);
  if (!stateCode || Number.isNaN(stateCode))
    return res.status(400).json({ error: "Invalid state code" });

  const startYear = parseInt(req.query.startYear) || 2009;
  const endYear = parseInt(req.query.endYear) || 2022;

  try {
    const topCategoriesData = await query(
      `
        SELECT 
          tc.category_name, 
          ROUND(AVG(amount), 2) AS avg_amount
        FROM ${taxData} td
        INNER JOIN ${taxCategory} tc ON tc.category_code = td.category_code
        WHERE state_code = :stateCode
        AND year >= :startYear
        AND year <= :endYear
        GROUP BY tc.category_name
        ORDER BY avg_amount DESC
      `,
      { stateCode, startYear, endYear }
    );

    if (topCategoriesData.length == 0) {
      return res.status(404).json({ error: "No data found for the state" });
    }

    const map = new Map();
    for (let row of topCategoriesData) {
      const { categoryName, avgAmount } = row;
      map.set(categoryName, avgAmount);
    }

    const payload = [["Tax Category Name", "Average Tax Revenue ($)"]];
    for (let key of map.keys()) {
      payload.push([key, map.get(key)]);
    }

    res.json(payload);
  } catch (err) {
    console.error("Failed to fetch top tax categories");
    console.error(err);
    res.status(500).json({ error: "Failed to fetch top tax categories" });
  }
});

taxDataRouter.get("/geo", async (req, res) => {
  const startYear = parseInt(req.query.startYear) || 2009;
  const endYear = parseInt(req.query.endYear) || 2022;

  try {
    const geoData = await query(
      `
        SELECT 
          s.state_name, 
          ROUND(AVG(amount), 2) AS avg_amount
        FROM ${taxData} td
        INNER JOIN ${state} s ON td.state_code = s.state_code
        AND year >= :startYear
        AND year <= :endYear
        GROUP BY s.state_name
        ORDER BY avg_amount DESC
      `,
      { startYear, endYear }
    );

    if (geoData.length == 0) {
      return res.status(404).json({ error: "No data found for the state" });
    }

    const map = new Map();
    for (let row of geoData) {
      const { stateName, avgAmount } = row;
      map.set(stateName, avgAmount);
    }

    const payload = [["State", "Average Tax Revenue across all categories($)"]];
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

module.exports = taxDataRouter;
