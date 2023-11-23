const { Router } = require("express");
const { query } = require("../lib/db");
const { businessData, industry, covidData, state } = require("../lib/table");

const payrollDataRouter = Router();

payrollDataRouter.get("/", async (req, res) => {
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
            ROUND(tp.total_payroll/te.total_employee, 2) AS avg_payroll_per_employee
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
      const { year, industryName, avgPayrollPerEmployee } = row;
      if (payrollMap.has(year)) {
        payrollMap.get(year)[industryName] = avgPayrollPerEmployee;
      } else {
        let obj = {};
        for (let name of allIndustryNames) {
          obj[name] = 0;
        }
        obj[industryName] = avgPayrollPerEmployee;
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

payrollDataRouter.get("/top-industries", async (req, res) => {
  const stateCode = parseInt(req.query.stateCode);
  if (!stateCode || Number.isNaN(stateCode))
    return res.status(400).json({ error: "Invalid state code" });

  const startYear = parseInt(req.query.startYear) || 2010;
  const endYear = parseInt(req.query.endYear) || 2021;

  try {
    const topIndustriesData = await query(
      `
        WITH AvgPayrollPerEmployee AS (
          SELECT 
            bd.year, 
            bd.state_code, 
            i.industry_name,
            SUM(bd.annual_payroll)/sum(bd.employee_count) as avg_payroll_per_employee
          FROM ${businessData} bd
          INNER JOIN ${industry} i ON i.industry_code = bd.industry_code
          WHERE bd.annual_payroll != 0 AND bd.annual_payroll IS NOT NULL
          AND bd.employee_count != 0 AND bd.employee_count IS NOT NULL
          GROUP BY bd.year, bd.state_code, i.industry_name
        )
        SELECT
          industry_name,
          ROUND(AVG(avg_payroll_per_employee), 2) as yearly_avg_payroll_per_employee
        FROM AvgPayrollPerEmployee
        WHERE state_code = :stateCode
        AND year >= :startYear
        AND year <= :endYear
        GROUP BY industry_name
        ORDER BY yearly_avg_payroll_per_employee DESC
      `,
      { stateCode, startYear, endYear }
    );

    if (topIndustriesData.length == 0) {
      return res.status(404).json({ error: "No data found for the state" });
    }

    const map = new Map();
    for (let row of topIndustriesData) {
      const { industryName, yearlyAvgPayrollPerEmployee } = row;
      map.set(industryName, yearlyAvgPayrollPerEmployee);
    }

    const payload = [["Industry Name", "Average Payroll per Employee ($1000)"]];
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

payrollDataRouter.get("/geo", async (req, res) => {
  const startYear = parseInt(req.query.startYear) || 2010;
  const endYear = parseInt(req.query.endYear) || 2021;

  try {
    const geoData = await query(
      `
        WITH AvgPayrollPerEmployee AS (
          SELECT 
            bd.year, 
            s.state_name, 
            bd.industry_code,
            SUM(bd.annual_payroll)/sum(bd.employee_count) as avg_payroll_per_employee
          FROM ${businessData} bd
          INNER JOIN ${state} s ON s.state_code = bd.state_code
          WHERE bd.annual_payroll != 0 AND bd.annual_payroll IS NOT NULL
          AND bd.employee_count != 0 AND bd.employee_count IS NOT NULL
          GROUP BY bd.year, s.state_name, bd.industry_code
        )
        SELECT
          state_name,
          ROUND(AVG(avg_payroll_per_employee), 2) as yearly_avg_payroll_per_employee
        FROM AvgPayrollPerEmployee
        WHERE year >= :startYear
        AND year <= :endYear
        GROUP BY state_name
        ORDER BY yearly_avg_payroll_per_employee DESC
      `,
      { startYear, endYear }
    );

    if (geoData.length == 0) {
      return res.status(404).json({ error: "No data found for the state" });
    }

    const map = new Map();
    for (let row of geoData) {
      const { stateName, yearlyAvgPayrollPerEmployee } = row;
      map.set(stateName, yearlyAvgPayrollPerEmployee);
    }

    const payload = [["State", "Average Payroll per Employee ($1000)"]];
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

module.exports = payrollDataRouter;
