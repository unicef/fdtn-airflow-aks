-- Create disaster table if does not exists
-- coalesce to replace NULL by 0


DROP TABLE IF EXISTS population_by_region;

CREATE TABLE population_by_region as (
SELECT disasters_hex.event_id ,
adm2_hex.gid2 AS gid2,
eapro_adm2."NAME_2" AS "NAME_2", 
coalesce(sum(population_crosstab.children_under_five),0) AS children_under_five, 
coalesce(sum(population_crosstab.elderly_60_plus),0) AS elderly_60_plus, 
coalesce(sum(population_crosstab.men),0) AS men, 
coalesce(sum(population_crosstab.women),0) AS women, 
coalesce(sum(population_crosstab.women_of_reproductive_age_15_49),0) AS women_of_reproductive_age_15_49, 
coalesce(sum(population_crosstab.youth_15_24),0) AS youth_15_24, 
coalesce(sum(population_crosstab.general),0) AS general, 
coalesce(sum(schools.count),0) AS school_count ,
coalesce(sum(hospitals.count),0) AS hospital_count 

FROM population_crosstab 
LEFT JOIN disasters_hex ON population_crosstab.h3_08 = disasters_hex.h3_08 
LEFT JOIN adm2_hex ON disasters_hex.h3_08 = adm2_hex.h3_08 
LEFT JOIN eapro_adm2 ON adm2_hex.gid2 = eapro_adm2."GID_2" 
LEFT JOIN schools ON schools.h3_08 = adm2_hex.h3_08 
LEFT JOIN hospitals ON hospitals.h3_08 = adm2_hex.h3_08 

GROUP BY disasters_hex.event_id, adm2_hex.gid2, eapro_adm2."NAME_2" 
ORDER BY children_under_five DESC 
);

CREATE INDEX ON population_by_region(event_id)
