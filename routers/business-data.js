const { Router } = require("express");
const { query } = require("../lib/db");
const { businessData, industry, covidData, state } = require("../lib/table");

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
          INNER JOIN 
              IndustryEstablishments ie 
              ON ie.year = te.year
              AND ie.state_code = te.state_code
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

businessDataRouter.get("/top-5-industries", async (req, res) => {
  const stateCode = parseInt(req.query.stateCode);
  if (!stateCode || Number.isNaN(stateCode))
    return res.status(400).json({ error: "Invalid state code" });

  const startYear = parseInt(req.query.startYear) || 2010;
  const endYear = parseInt(req.query.endYear) || 2021;

  try {
    const topIndustriesData = await query(
      `
        WITH MaxIndustry AS (
          SELECT 
              industry_code, 
              SUM(count_establishments) as count_establishments
          FROM ${businessData}
          WHERE state_code = :stateCode
          AND count_establishments IS NOT NULL
          AND year >= :startYear
          AND year <= :endYear
          GROUP BY industry_code
          ORDER BY count_establishments DESC
          FETCH FIRST 5 ROWS ONLY
        )
        SELECT 
          m.industry_code, i.industry_name, m.count_establishments
        FROM MaxIndustry m
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

    const payload = [
      ["Industry Name", "Total Count of Business Establishments"],
    ];
    for (let key of map.keys()) {
      payload.push([key, map.get(key)]);
    }

    res.json(payload);
  } catch (err) {
    console.error("Failed to fetch top 5 industries");
    console.error(err);
    res.status(500).json({ error: "Failed to fetch top 5 industries" });
  }
});

businessDataRouter.get("/geo", async (req, res) => {
  const startYear = parseInt(req.query.startYear) || 2010;
  const endYear = parseInt(req.query.endYear) || 2021;

  try {
    const geoData = await query(
      `
        SELECT 
            s.state_name,
            SUM(bd.count_establishments) as total_establishments
        FROM ${businessData} bd
        INNER JOIN ${state} s 
        ON s.state_code = bd.state_code
        WHERE bd.year >= :startYear
        AND bd.year <= :endYear
        AND bd.count_establishments IS NOT NULL
        GROUP BY bd.state_code, s.state_name
        ORDER BY total_establishments DESC 
      `,
      { startYear, endYear }
    );

    if (geoData.length == 0) {
      return res.status(404).json({ error: "No data found for the state" });
    }

    const map = new Map();
    for (let row of geoData) {
      const { stateName, totalEstablishments } = row;
      map.set(stateName, totalEstablishments);
    }

    const payload = [["State", "Total Count of Business Establishments"]];
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

businessDataRouter.get("/payroll", async (req, res) => {
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
    const [allCovidData, allIndustries, allPayrollData] = await Promise.all([
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
          WITH TotalPayroll AS (
              SELECT 
                  year, 
                  state_code, 
                  industry_code, 
                  SUM(annual_payroll) AS total_payroll
              FROM ${businessData}
              WHERE annual_payroll != 0
              AND annual_payroll IS NOT NULL
              GROUP BY year, state_code, industry_code
          ), 
          TotalEmployee AS (
              SELECT 
                  year, 
                  state_code, 
                  industry_code, 
                  SUM(employee_count) AS total_employee
              FROM ${businessData}
              WHERE employee_count != 0
              AND employee_count IS NOT NULL
              GROUP BY year, state_code, industry_code
          )
          SELECT 
              tp.year, 
              i.industry_name, 
              ROUND(tp.total_payroll/te.total_employee, 2) AS avg_payroll
          FROM TotalPayroll tp
          INNER JOIN TotalEmployee te
          ON te.year = tp.year
          AND te.state_code = tp.state_code
          AND te.industry_code = tp.industry_code
          INNER JOIN ${industry} i ON i.industry_code = tp.industry_code
          WHERE tp.state_code = :stateCode
          AND tp.year >= :startYear
          AND tp.year <= :endYear
          ${
            includedIndustries.length != 0
              ? `AND tp.industry_code IN (${includedIndustriesPlaceholder})`
              : ""
          }
          ORDER BY tp.year ASC
        `,
        includedIndustries.length == 0
          ? { stateCode, startYear, endYear }
          : { stateCode, startYear, endYear, ...includedIndustriesBind }
      ),
    ]);

    if (allPayrollData.length == 0) {
      return res.status(404).json({ error: "No data found for the state" });
    }

    const allIndustryNames = allIndustries.map((i) => i.industryName);

    const covidDataMap = new Map();
    for (let row of allCovidData) {
      const { year, countCases } = row;
      covidDataMap.set(year, countCases);
    }

    const payrollMap = new Map();
    for (let row of allPayrollData) {
      const { year, industryName, avgPayroll } = row;
      if (payrollMap.has(year)) {
        payrollMap.get(year)[industryName] = avgPayroll;
      } else {
        let obj = {};
        for (let name of allIndustryNames) {
          obj[name] = 0;
        }
        obj[industryName] = avgPayroll;
        payrollMap.set(year, obj);
      }
    }

    let payload = [["Year", "Confirmed COVID-19 Cases", ...allIndustryNames]];
    for (let year of payrollMap.keys()) {
      payload.push([
        year,
        covidDataMap.has(year) ? covidDataMap.get(year) : 0,
        ...Object.values(payrollMap.get(year)),
      ]);
    }
    res.json(payload);
  } catch (err) {
    console.error("Failed to fetch payroll data");
    console.error(err);
    res.status(500).json({ error: "Failed to fetch payroll data" });
  }
});

businessDataRouter.get("/payroll/top-5-industries", async (req, res) => {
  const stateCode = parseInt(req.query.stateCode);
  if (!stateCode || Number.isNaN(stateCode))
    return res.status(400).json({ error: "Invalid state code" });

  const startYear = parseInt(req.query.startYear) || 2010;
  const endYear = parseInt(req.query.endYear) || 2021;

  try {
    const topIndustriesData = await query(
      `
        SELECT 
            i.industry_name,
            SUM(bd.annual_payroll) / SUM(bd.employee_count) AS average_payroll
        FROM ${businessData} bd
        INNER JOIN ${industry} i 
        ON bd.industry_code = i.industry_code
        WHERE bd.year >= :startYear 
        AND bd.year <= :endYear
        AND bd.state_code = :stateCode
        AND bd.annual_payroll != 0 
        AND bd.annual_payroll IS NOT NULL
        AND bd.employee_count != 0 
        AND bd.employee_count IS NOT NULL
        GROUP BY i.industry_name
        ORDER BY average_payroll DESC
        FETCH FIRST 5 ROWS ONLY
      `,
      { stateCode, startYear, endYear }
    );

    if (topIndustriesData.length == 0) {
      return res.status(404).json({ error: "No data found for the state" });
    }

    const map = new Map();
    for (let row of topIndustriesData) {
      const { industryName, averagePayroll } = row;
      map.set(industryName, averagePayroll);
    }

    const payload = [
      ["Industry Name", "Average Payroll per Employee (in $1000)"],
    ];
    for (let key of map.keys()) {
      payload.push([key, map.get(key)]);
    }

    res.json(payload);
  } catch (err) {
    console.error("Failed to fetch top 5 industries");
    console.error(err);
    res.status(500).json({ error: "Failed to fetch top 5 industries" });
  }
});

businessDataRouter.get("/payroll/geo", async (req, res) => {
  const startYear = parseInt(req.query.startYear) || 2010;
  const endYear = parseInt(req.query.endYear) || 2021;

  try {
    const geoData = await query(
      `
        SELECT 
            s.state_name,
            SUM(bd.annual_payroll) / SUM(bd.employee_count) AS average_payroll
        FROM ${businessData} bd
        INNER JOIN ${state} s 
        ON bd.state_code = s.state_code
        WHERE bd.year >= :startYear 
        AND bd.year <= :endYear
        AND bd.annual_payroll != 0 
        AND bd.annual_payroll IS NOT NULL
        AND bd.employee_count != 0 
        AND bd.employee_count IS NOT NULL
        GROUP BY s.state_name
        ORDER BY average_payroll DESC
      `,
      { startYear, endYear }
    );

    if (geoData.length == 0) {
      return res.status(404).json({ error: "No data found for the state" });
    }

    const map = new Map();
    for (let row of geoData) {
      const { stateName, averagePayroll } = row;
      map.set(stateName, averagePayroll);
    }

    const payload = [["State", "Average Payroll per Employee (in $1000)"]];
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
