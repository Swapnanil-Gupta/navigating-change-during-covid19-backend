const config = {
  port: process.env.PORT,
  corsAllowedOrigin: process.env.CORS_ALLOWED_ORIGIN,
  dbTablePrefix: process.env.DB_TABLE_PREFIX,
  dbUser: process.env.DB_USER,
  dbPassword: process.env.DB_PASSWORD,
  dbConnectString: process.env.DB_CONNECT_STRING,
};

module.exports = config;
