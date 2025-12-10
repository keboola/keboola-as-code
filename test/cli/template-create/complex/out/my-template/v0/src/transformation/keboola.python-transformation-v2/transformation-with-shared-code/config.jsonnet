{
  parameters: {
    blocks: [
      {
        name: "Block 1",
        codes: [
          {
            name: "Shared Code Used",
            script: [
              "{{" + ConfigRowId("code-with-variables") + "}}",
            ],
          },
        ],
      },
    ],
    packages: [
      "xgboost",
    ],
  },
  shared_code_path: "_shared/keboola.python-transformation-v2",
}
