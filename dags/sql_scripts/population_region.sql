-- Create disaster table if does not exists
-- coalesce to replace NULL by 0


TRUNCATE population_by_region_wp;

INSERT INTO population_by_region (
SELECT disasters_hex.event_id ,

adm2_hex.gid0 AS gid0,
eapro_adm2."NAME_0" AS "NAME_0", 

adm2_hex.gid1 AS gid1,
eapro_adm2."NAME_1" AS "NAME_1", 
  
adm2_hex.gid2 AS gid2,
eapro_adm2."NAME_2" AS "NAME_2", 
coalesce(sum(pc.children_under_1),0) AS children_under_1, 
coalesce(sum(pc.children_1_4),0) AS children_1_4, 
coalesce(sum(pc.children_5_19),0) AS children_5_19, 
coalesce(sum(pc.children_0_19),0) as children_0_19, 
coalesce(sum(pc.elderly_60_plus),0) AS elderly_60_plus,
coalesce(sum(pc.men),0) AS men, 
coalesce(sum(pc.women),0) AS women, 
coalesce(sum(pc.women_15_49),0) AS women_15_49, 
coalesce(sum(pc.general),0) AS general, 
coalesce(sum(schools.count),0) AS school_count ,
coalesce(sum(hospitals.count),0) AS hospital_count 

FROM public.worldpop_h3_08 pc
LEFT JOIN disasters_hex ON pc.h3_08 = disasters_hex.h3_08 
LEFT JOIN adm2_hex ON disasters_hex.h3_08 = adm2_hex.h3_08 
LEFT JOIN eapro_adm2 ON adm2_hex.gid2 = eapro_adm2."GID_2" 
LEFT JOIN schools ON schools.h3_08 = adm2_hex.h3_08 
LEFT JOIN hospitals ON hospitals.h3_08 = adm2_hex.h3_08 

GROUP BY disasters_hex.event_id, adm2_hex.gid2, eapro_adm2."NAME_2" , adm2_hex.gid0, eapro_adm2."NAME_0" , adm2_hex.gid1, eapro_adm2."NAME_1" 
ORDER BY children_under_1 DESC

);

CREATE INDEX ON population_by_region_wp(event_id)
