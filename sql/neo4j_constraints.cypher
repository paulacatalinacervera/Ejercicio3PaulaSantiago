CREATE CONSTRAINT zone_id IF NOT EXISTS
FOR (z:Zone) REQUIRE z.location_id IS UNIQUE;

CREATE INDEX zone_borough IF NOT EXISTS
FOR (z:Zone) ON (z.borough);
