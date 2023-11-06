const { dbTablePrefix } = require("./config");

const prefixTable = (table) => `${dbTablePrefix}.${table}`;

const table = {
  state: prefixTable("State"),
  businessSize: prefixTable("Business_Size"),
  businessType: prefixTable("Business_Type"),
  industry: prefixTable("Industry"),
  businessData: prefixTable("Business_Data"),
};

module.exports = table;
