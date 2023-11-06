const { getConnection, OUT_FORMAT_OBJECT } = require("oracledb");
const camelCaseKeys = require("camelcase-keys");
const { dbUser, dbPassword, dbConnectString } = require("./config");

const query = async (query, bindParams = []) => {
  let connection = null;
  try {
    connection = await getConnection({
      user: dbUser,
      password: dbPassword,
      connectString: dbConnectString,
    });
    const result = await connection.execute(query, bindParams, {
      outFormat: OUT_FORMAT_OBJECT,
    });
    return camelCaseKeys(result.rows);
  } catch (err) {
    throw err;
  } finally {
    if (connection) await connection.close();
  }
};

module.exports = { query };
