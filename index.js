require("dotenv").config();
const express = require("express");
const cors = require("cors");
const { port, corsAllowedOrigin } = require("./lib/config");
const rootRouter = require("./routers/root");
const businessDataRouter = require("./routers/business-data");
const emissionDataRouter = require("./routers/emission-data");
const payrollDataRouter = require("./routers/payroll-data");
const taxDataRouter = require("./routers/tax-data");
const entrepreneurshipDataRouter = require("./routers/entrepreneurship-data");

const app = express();
app.use(express.json());
app.use(
  cors({
    origin: corsAllowedOrigin,
  })
);

app.use("/", rootRouter);
app.use("/business-data", businessDataRouter);
app.use("/emission-data", emissionDataRouter);
app.use("/payroll-data", payrollDataRouter);
app.use("/tax-data", taxDataRouter);
app.use("/entrepreneurship-data", entrepreneurshipDataRouter);

app.listen(port, () => console.log(`Server is listening on port ${port}`));
