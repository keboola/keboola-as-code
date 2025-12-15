package schema

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
)

func TestGenerateDocument(t *testing.T) {
	t.Parallel()
	document, err := GenerateDocument(getSampleSchema())
	documentJSON := json.MustEncodeString(document, true)
	require.NoError(t, err)

	expected := `
{
  "nc:Currency": "EUR",
  "nc:CurrencyCode": "EUR",
  "nc:Amount": 0,
  "nc:IdentificationID": "",
  "nc:Vehicle": {
    "@base": "",
    "@id": "",
    "nc:VehicleAxleQuantity": 20,
    "nc:VehicleIdentification": {
      "nc:IdentificationID": ""
    },
    "nc:VehicleMSRPAmount": {
      "nc:Amount": 0,
      "nc:Currency": "EUR"
    }
  },
  "nc:VehicleAxleQuantity": 20,
  "nc:VehicleIdentification": {
    "nc:IdentificationID": ""
  },
  "nc:VehicleMSRPAmount": {
    "nc:Amount": 0,
    "nc:Currency": "EUR"
  }
}
`
	assert.JSONEq(t, strings.TrimSpace(expected), strings.TrimSpace(documentJSON))
}

func TestGenerateDocumentEmptySchema(t *testing.T) {
	t.Parallel()
	document, err := GenerateDocument([]byte(`{}`))
	documentJSON := json.MustEncodeString(document, true)
	require.NoError(t, err)
	assert.JSONEq(t, `{}`, documentJSON)
}

func TestGenerateDocumentWithMinItems(t *testing.T) {
	t.Parallel()
	schemaJSON := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "object",
		"properties": {
			"blocks": {
				"type": "array",
				"minItems": 1,
				"items": {
					"type": "object",
					"properties": {
						"name": {
							"type": "string"
						},
						"codes": {
							"type": "array",
							"minItems": 1,
							"items": {
								"type": "object",
								"properties": {
									"name": {
										"type": "string"
									},
									"script": {
										"type": "array",
										"items": {
											"type": "string"
										}
									}
								}
							}
						}
					}
				}
			},
			"packages": {
				"type": "array",
				"items": {
					"type": "string"
				}
			}
		}
	}`
	document, err := GenerateDocument([]byte(schemaJSON))
	require.NoError(t, err)
	documentJSON := json.MustEncodeString(document, true)

	expected := `{
		"blocks": [
			{
				"name": "",
				"codes": [
					{
						"name": "",
						"script": []
					}
				]
			}
		],
		"packages": []
	}`
	assert.JSONEq(t, expected, documentJSON)
}

func getSampleSchema() []byte {
	return []byte(`
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "additionalProperties": false,
  "properties": {
    "nc:Vehicle": {
      "description": "A conveyance designed to carry an operator, passengers and/or cargo, over land.",
      "oneOf": [
        {
          "$ref": "#/definitions/nc:VehicleType"
        },
        {
          "type": "array",
          "items": {
            "$ref": "#/definitions/nc:VehicleType"
          }
        }
      ]
    },
    "nc:VehicleAxleQuantity": {
      "description": "A count of common axles of rotation of one or more wheels of a vehicle, whether power driven or freely rotating.",
      "default": 20,
      "oneOf": [
        {
          "$ref": "#/definitions/niem-xs:nonNegativeInteger"
        },
        {
          "type": "array",
          "items": {
            "$ref": "#/definitions/niem-xs:nonNegativeInteger"
          }
        }
      ]
    },
    "nc:VehicleMSRPAmount": {
      "description": "A manufacturer's suggested retail price of a vehicle; a price at which a manufacturer recommends a vehicle be sold.",
      "oneOf": [
        {
          "$ref": "#/definitions/nc:AmountType"
        },
        {
          "type": "array",
          "items": {
            "$ref": "#/definitions/nc:AmountType"
          }
        }
      ]
    },
    "nc:Amount": {
      "description": "An amount of money.",
      "oneOf": [
        {
          "$ref": "#/definitions/niem-xs:decimal"
        },
        {
          "type": "array",
          "items": {
            "$ref": "#/definitions/niem-xs:decimal"
          }
        }
      ]
    },
    "nc:Currency": {
      "propertyOrder": 1,
      "description": "A data concept for a unit of money or exchange.",
      "oneOf": [
        {
          "anyOf": [
            {
              "$ref": "#/properties/nc:CurrencyCode"
            }
          ]
        },
        {
          "type": "array",
          "items": {
            "anyOf": [
              {
                "$ref": "#/properties/nc:CurrencyCode"
              }
            ]
          }
        }
      ]
    },
    "nc:CurrencyCode": {
      "propertyOrder": 2,
      "description": "A unit of money or exchange.",
      "oneOf": [
        {
          "$ref": "#/definitions/iso_4217:CurrencyCodeType"
        },
        {
          "type": "array",
          "items": {
            "$ref": "#/definitions/iso_4217:CurrencyCodeType"
          }
        }
      ]
    },
    "nc:VehicleIdentification": {
      "description": "A unique identification for a specific vehicle.",
      "oneOf": [
        {
          "$ref": "#/definitions/nc:IdentificationType"
        },
        {
          "type": "array",
          "items": {
            "$ref": "#/definitions/nc:IdentificationType"
          }
        }
      ]
    },
    "nc:IdentificationID": {
      "description": "An identifier.",
      "oneOf": [
        {
          "$ref": "#/definitions/niem-xs:string"
        },
        {
          "type": "array",
          "items": {
            "$ref": "#/definitions/niem-xs:string"
          }
        }
      ]
    }
  },
  "definitions": {
    "nc:VehicleType": {
      "description": "A data type for a conveyance designed to carry an operator, passengers and/or cargo, over land.",
      "allOf": [
        {
          "$ref": "#/definitions/nc:ConveyanceType"
        },
        {
          "type": "object",
          "properties": {
            "nc:VehicleAxleQuantity": {
              "$ref": "#/properties/nc:VehicleAxleQuantity"
            },
            "nc:VehicleIdentification": {
              "$ref": "#/properties/nc:VehicleIdentification"
            },
            "nc:VehicleMSRPAmount": {
              "$ref": "#/properties/nc:VehicleMSRPAmount"
            }
          }
        }
      ]
    },
    "nc:ConveyanceType": {
      "description": "A data type for a means of transport from place to place.",
      "allOf": [
        {
          "$ref": "#/definitions/_base"
        },
        {
          "$ref": "#/definitions/nc:ItemType"
        },
        {
          "type": "object",
          "properties": {}
        }
      ]
    },
    "nc:ItemType": {
      "description": "A data type for an article or thing.",
      "allOf": [
        {
          "$ref": "#/definitions/_base"
        },
        {
          "type": "object",
          "properties": {}
        }
      ]
    },
    "nc:AmountType": {
      "description": "A data type for an amount of money.",
      "type": "object",
      "properties": {
        "nc:Amount": {
          "$ref": "#/properties/nc:Amount"
        },
        "nc:Currency": {
          "$ref": "#/properties/nc:Currency"
        }
      }
    },
    "iso_4217:CurrencyCodeType": {
      "description": "A data type for a currency that qualifies a monetary amount.",
      "oneOf": [
        {
          "$ref": "#/definitions/iso_4217:CurrencyCodeSimpleType"
        },
        {
          "type": "object",
          "properties": {
            "rdf:value": {
              "$ref": "#/definitions/iso_4217:CurrencyCodeSimpleType"
            }
          }
        }
      ]
    },
    "iso_4217:CurrencyCodeSimpleType": {
      "type": "string",
      "description": "A data type for a currency that qualifies a monetary amount.",
      "oneOf": [
        {
          "enum": [
            "EUR"
          ],
          "description": "Euro"
        },
        {
          "enum": [
            "GBP"
          ],
          "description": "Pound Sterling"
        },
        {
          "enum": [
            "USD"
          ],
          "description": "US Dollar"
        }
      ]
    },
    "nc:IdentificationType": {
      "description": "A data type for a representation of an identity.",
      "type": "object",
      "properties": {
        "nc:IdentificationID": {
          "$ref": "#/properties/nc:IdentificationID"
        }
      }
    },
    "niem-xs:decimal": {
      "description": "A data type for arbitrary precision decimal numbers.",
      "type": "number"
    },
    "niem-xs:nonNegativeInteger": {
      "description": "A data type for an integer with a minimum value of 0.",
      "type": "number"
    },
    "niem-xs:string": {
      "description": "A data type for character strings in XML.",
      "type": "string"
    },
    "_base": {
      "type": "object",
      "patternProperties": {
        "^ism:.*": {
          "type": "string"
        },
        "^ntk:.*": {
          "type": "string"
        }
      },
      "properties": {
        "@id": {
          "format": "uriref"
        },
        "@base": {
          "format": "uriref"
        }
      }
    }
  }
}
`)
}
