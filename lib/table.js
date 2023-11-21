const { dbTablePrefix } = require("./config");

const prefixTable = (table) => `${dbTablePrefix}.${table}`;

const table = {
  state: prefixTable("State"),
  businessSize: prefixTable("Business_Size"),
  businessType: prefixTable("Business_Type"),
  industry: prefixTable("Industry"),
  businessData: prefixTable("Business_Data"),
  energySector: prefixTable("Energy_Sector"),
  fuelType: prefixTable("Fuel_Type"),
  emissionData: prefixTable("Emissions"),
  covidData: prefixTable("Covid_Data"),
  taxCategory: prefixTable("Tax_Category"),
  taxData: prefixTable("Tax_Data"),
};

module.exports = table;
