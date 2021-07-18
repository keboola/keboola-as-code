CREATE OR REPLACE TABLE "test" (
  "id" number default null,
  "orderstatus" varchar(100) default null,
  "price" float
);

INSERT INTO "test" ("id", "orderstatus", "price") VALUES(123, 'ok', 12.34);

INSERT INTO "test" ("id", "orderstatus", "price") VALUES(456, 'ko', 45.67);

DROP TABLE IF EXISTS "test";
