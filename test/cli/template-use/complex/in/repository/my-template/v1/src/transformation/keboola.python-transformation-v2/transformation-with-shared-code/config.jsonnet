{
  parameters: {
    blocks: [
      {
        "name": "Block 1",
        "codes": [
          {
            "name": "Shared Code Used",
            "script": [
              "print('Hello from shared code')"
            ]
          }
        ]
      }
    ],
    packages: [
      "xgboost"
    ]
  },
  shared_code_path: "_shared/keboola.python-transformation-v2"
}
