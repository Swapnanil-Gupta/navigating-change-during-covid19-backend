const { Router } = require("express");
const { query } = require("../lib/db");
const {
  businessData,
  industry,
  covidData,
  businessSize,
  businessType,
  emissionData,
  energySector,
  fuelType,
  state,
  taxCategory,
  taxData,
} = require("../lib/table");

const rootRouter = Router();

rootRouter.get("/tuples", async (req, res) => {
  const data = await query(
    `
      WITH countState AS (
          SELECT DISTINCT COUNT(*) AS count_state FROM ${state}
      ),
      countBusinessType AS (
          SELECT DISTINCT COUNT(*) AS count_business_type FROM ${businessType}
      ),
      countBusinessSize AS (
          SELECT DISTINCT COUNT(*) AS count_business_size FROM ${businessSize}
      ),
      countIndustry AS (
          SELECT DISTINCT COUNT(*) AS count_industry FROM ${industry}
      ),
      countBusinessData AS (
          SELECT DISTINCT COUNT(*) AS count_business_data FROM ${businessData}
      ), 
      countCovidData AS (
          SELECT DISTINCT COUNT(*) AS count_covid_data FROM ${covidData}
      ),
      countEnergySector AS (
          SELECT DISTINCT COUNT(*) AS count_energy_sector FROM ${energySector}
      ),
      countFuelType AS (
          SELECT DISTINCT COUNT(*) AS count_fuel_type FROM ${fuelType}
      ),
      countEmissionData AS (
          SELECT DISTINCT COUNT(*) AS count_emission_data FROM ${emissionData}
      ),
      countTaxCategory AS (
          SELECT DISTINCT COUNT(*) AS count_tax_category FROM ${taxCategory}
      ),
      countTaxData AS (
          SELECT DISTINCT COUNT(*) AS count_tax_data FROM ${taxData}
      )
      SELECT * 
      FROM 
          countState,
          countCovidData,
          countIndustry,
          countBusinessSize,
          countBusinessType,
          countBusinessData,
          countEnergySector, 
          countFuelType, 
          countEmissionData, 
          countTaxCategory,
          countTaxData 
    `
  );

  let total = 0;
  for (let prop in data[0]) total += data[0][prop];
  res.json(total);
});

rootRouter.get("/state", async (req, res) => {
  try {
    const data = await query(`
      SELECT UNIQUE state_code, state_name
      FROM ${state}
      ORDER BY state_name ASC
    `);
    res.json(data);
  } catch (err) {
    console.error("Failed to fetch states");
    console.error(err);
    res.status(500).json({ error: "Failed to fetch states" });
  }
});

rootRouter.get("/business-industry", async (req, res) => {
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

rootRouter.get("/energy-sector", async (req, res) => {
  try {
    const data = await query(`
      SELECT UNIQUE sector_code, sector_name
      FROM ${energySector}
      ORDER BY sector_name ASC
    `);
    res.json(data);
  } catch (err) {
    console.error("Failed to fetch energy sectors");
    console.error(err);
    res.status(500).json({ error: "Failed to fetch energy sectors" });
  }
});

rootRouter.get("/tax-category", async (req, res) => {
  try {
    const data = await query(`
      SELECT UNIQUE category_code, category_name
      FROM ${taxCategory}
      ORDER BY category_name ASC
    `);
    res.json(data);
  } catch (err) {
    console.error("Failed to fetch tax categories");
    console.error(err);
    res.status(500).json({ error: "Failed to fetch tax categories" });
  }
});

rootRouter.get("/business-size", async (req, res) => {
  try {
    const data = await query(`
      SELECT UNIQUE size_code, size_name
      FROM ${businessSize}
      ORDER BY size_name ASC
    `);
    res.json(data);
  } catch (err) {
    console.error("Failed to fetch business sizes");
    console.error(err);
    res.status(500).json({ error: "Failed to fetch business sizes" });
  }
});

module.exports = rootRouter;
