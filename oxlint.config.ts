import { defineConfig } from "oxlint"

export default defineConfig({
  ignorePatterns: [".claude/**", ".agents/**", "dist/**", "node_modules/**", "tools/oxlint/anti-slop/**"],
  jsPlugins: [
    { name: "anti-slop", specifier: "./tools/oxlint/anti-slop/index.ts" },
    { name: "anti-slop-effect", specifier: "./tools/oxlint/anti-slop/effect/index.ts" }
  ],
  rules: {
    "oxc/no-accumulating-spread": "error",
    "anti-slop/no-array-filter-map": "error",
    "anti-slop/no-reduce-accumulator-copy": "error",
    "anti-slop/no-chained-type-assertions": "error",
    "anti-slop/no-conditional-empty-object-spread": "error",
    "anti-slop/no-known-value-widening": "error",
    "anti-slop/no-module-mocking": "error",
    "anti-slop/no-object-parameters": "error",
    "anti-slop/no-reflect-apply": "error",
    "anti-slop/no-reflect-get": "error",
    "anti-slop/no-runtime-typeof": "error",
    "anti-slop/no-shape-in-symbol-names": "error",
    "anti-slop/no-unknown-parameters": "error",
    "anti-slop/no-unknown-returns": "error",
    "anti-slop/no-unknown-type-aliases": "error",
    "anti-slop/no-unsafe-dictionary-type": "error",
    "anti-slop/no-widen-then-assert": "error",
    "anti-slop/require-readable-spacing": "error",
    "anti-slop/require-safety-comment-for-type-assertion": "error",
    "anti-slop-effect/no-manual-effect-error-tag": "error",
    "anti-slop-effect/no-manual-tag-comparison": "error",
    "anti-slop-effect/no-manual-tagged-construction": "error",
    "anti-slop-effect/no-service-constructor-imports": "error",
    "anti-slop-effect/prefer-effect-match": "error"
  },
  overrides: [
    {
      // Every packet on the wire passes through these files. Effect tagged constructors plus Match dispatch
      // measured 4 to 6 times slower than plain objects with a switch (Bun 44M vs 7.7M ops/s, Node 16M vs
      // 4.3M, median of 5 runs), so the wire layer keeps plain tagged objects.
      files: ["src/packet.ts", "src/codec.ts", "src/session.ts"],
      rules: {
        "anti-slop-effect/no-manual-tag-comparison": "off",
        "anti-slop-effect/no-manual-tagged-construction": "off",
        "anti-slop-effect/prefer-effect-match": "off"
      }
    }
  ]
})
