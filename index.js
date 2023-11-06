require("dotenv").config();
const express = require("express");
const cors = require("cors");
const { port, corsAllowedOrigin } = require("./lib/config");
const stateRouter = require("./routers/state");
const businessDataRouter = require("./routers/business-data");

const app = express();
app.use(express.json());
app.use(
  cors({
    origin: corsAllowedOrigin,
  })
);

app.use("/state", stateRouter);
app.use("/business-data", businessDataRouter);

app.listen(port, () => console.log(`Server is listening on port ${port}`));
