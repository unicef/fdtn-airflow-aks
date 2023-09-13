-- TRUNCATE TABLE AND INSERT

TRUNCATE public.meta_movement_formatted;

INSERT INTO public.meta_movement_formatted (

SELECT mm.disaster_id
,mm.disaster_name
,mm.country
, ah_start.name0 as start_adm0
, ah_start.name1 as start_adm1
, ah_start.name2 as start_adm2

, ah_end.name0 as end_adm0
, ah_end.name1 as end_adm1
, ah_end.name2 as end_adm2
,sum(mm.n_difference)

FROM private.meta_movement_test_h308 mm 

left join  public.adm2_hex ah_start
on ah_start.h3_08= mm.start_h3_08 

left join  public.adm2_hex ah_end
on ah_end.h3_08= mm.end_h3_08 

group by 1,2,3,4,5,6,7,8,9)
;

CREATE INDEX ON public.meta_movement_formatted(start_adm2)
