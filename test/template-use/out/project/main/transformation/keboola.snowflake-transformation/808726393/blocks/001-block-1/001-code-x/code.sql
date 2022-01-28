CREATE OR REPLACE TABLE "808726406" (
  "id" number default null,
  "orderstatus" varchar(100) default null,
  "price" float
);

INSERT INTO "808726406" ("id", "orderstatus", "price") VALUES(123, 'ok', 12.34);

INSERT INTO "808726406" ("id", "orderstatus", "price") VALUES(456, 'ko', 45.67);

DROP TABLE IF EXISTS "808726406";
