SELECT
  s.serialnumber,
  s.sensorreadingtime,
  s.distancefromobject,
  a.x,
  a.y,
  a.z
FROM s
INNER JOIN c
  ON s.serialnumber = c.serialnumber
INNER JOIN a
  ON s.sensorreadingtime = a.timestamp