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

module.exports = rootRouter;
