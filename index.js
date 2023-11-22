require("dotenv").config();
const express = require("express");
const cors = require("cors");
const { port, corsAllowedOrigin } = require("./lib/config");
const stateRouter = require("./routers/state");
const businessDataRouter = require("./routers/business-data");
const emissionDataRouter = require("./routers/emission-data");
const rootRouter = require("./routers/root");
const taxDataRouter = require("./routers/tax-data")

const app = express();
app.use(express.json());
app.use(
  cors({
    origin: corsAllowedOrigin,
  })
);

app.use("/", rootRouter);
app.use("/state", stateRouter);
app.use("/business-data", businessDataRouter);
app.use("/emission-data", emissionDataRouter);
app.use("/tax-data",taxDataRouter)

app.listen(port, () => console.log(`Server is listening on port ${port}`));
