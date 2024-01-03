-- Create disaster table if does not exists
-- coalesce to replace NULL by 0
TRUNCATE population_by_region;

INSERT INTO population_by_region (

SELECT disasters_hex.event_id ,
pi.gid2 AS gid2,
pi."NAME_2" AS "NAME_2", 
coalesce(sum(pi.children_under_five),0) AS children_under_five,
coalesce(sum(pi.elderly_60_plus),0) AS elderly_60_plus, 
coalesce(sum(pi.men),0) AS men, 
coalesce(sum(pi.women),0) AS women, 
coalesce(sum(pi.women_of_reproductive_age_15_49),0) AS women_of_reproductive_age_15_49, 
coalesce(sum(pi.youth_15_24),0) AS youth_15_24, 
coalesce(sum(pi.general),0) AS general,
coalesce(sum(pi.school_count),0) AS school_count ,
coalesce(sum(pi.hospital_count),0) AS hospital_count 

FROM disasters_hex 
LEFT JOIN public.population_inter pi ON pi.h3_08 = disasters_hex.h3_08 


GROUP BY disasters_hex.event_id, pi.gid2, pi."NAME_2" 
ORDER BY children_under_five DESC 
);

CREATE INDEX ON population_by_region(event_id)
