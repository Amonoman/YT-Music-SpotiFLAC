const esbuild = require("esbuild");
const fs = require("fs");
const path = require("path");

const outDir = "dist";

esbuild.build({
  entryPoints: ["src/index.ts"],
  bundle: true,
  outfile: path.join(outDir, "index.js"),
  format: "iife",
  platform: "neutral",
  target: "es2020",
  globalName: "extension_logic",
  banner: {
    js: "var window = this; var self = this; var global = this;"
  }
}).then(() => {
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
  fs.copyFileSync("manifest.json", path.join(outDir, "manifest.json"));
  console.log("Build complete!");
}).catch((err) => {
  console.error("Build failed:", err);
  process.exit(1);
});
