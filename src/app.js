const express = require("express");
const helmet = require("helmet");
const cors = require("cors");
const morgan = require("morgan");

const env = require("./config/env");
const routes = require("./routes");
const notFound = require("./middleware/notFound");
const errorHandler = require("./middleware/errorHandler");

const app = express();

app.use(helmet());
app.use(
  cors({
    origin(origin, callback) {
      if (!origin) {
        callback(null, true);
        return;
      }

      if (env.frontendOrigins.includes(origin)) {
        callback(null, true);
        return;
      }

      callback(new Error(`CORS origin not allowed: ${origin}`));
    },
    credentials: true
  })
);
app.use(express.json());
app.use(morgan("dev"));

app.get("/health", (_req, res) => res.json({ ok: true }));

app.use("/api", routes);

app.use(notFound);
app.use(errorHandler);

module.exports = app;
