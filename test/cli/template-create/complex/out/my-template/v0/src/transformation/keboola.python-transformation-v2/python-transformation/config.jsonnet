{
  parameters: {
    blocks: [
      {
        name: "Block 1",
        codes: [
          {
            name: "Code X",
            script: [
              "print(100)\n",
            ],
          },
          {
            name: "Code Y",
            script: [
              "print(200)\n\t",
            ],
          },
        ],
      },
      {
        name: "Block 2",
        codes: [
          {
            name: "Code Z",
            script: [
              '# Sum of natural numbers up to num\n\nnum = 16\n\nif num < 0:\n    print("Enter a positive number")\nelse:\n    sum = 0\n    # use while loop to iterate until zero\n    while (num > 0):\n        sum += num\n        num -= 1\n    print("The sum is", sum)',
            ],
          },
        ],
      },
    ],
    packages: [
      "xgboost",
    ],
  },
}
