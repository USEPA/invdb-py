-- (putting this here as a record of what I ran on test for when we need to do it on UAT)

-- create new table activity_key with same columns as emissions_key
CREATE TABLE ggds_invdb.activity_key AS
SELECT * FROM ggds_invdb.emissions_key
WHERE false;
