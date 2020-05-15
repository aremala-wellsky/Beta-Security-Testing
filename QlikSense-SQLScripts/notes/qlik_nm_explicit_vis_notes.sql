/* With Explicit Visibility */
-- Rows: 20,173 | Distinct Rows: 9,263 | Time: 6.3 secs
 SELECT DISTINCT ee.entry_exit_id, uat.tier_link, virt_field_name, answer_val, date_effective
 FROM qlik_answer_access qaa 
 LEFT JOIN qlik_answer_vis_provider qap ON (qaa.visibility_id = qap.visibility_id)
 JOIN tmp_relevant_ees ee ON (ee.client_id = qaa.client_id AND qaa.date_effective::DATE <= ee.entry_date::DATE)
 JOIN qlik_user_access_tier_view uat ON (uat.user_access_tier != 1)
 WHERE ee.provider_id = uat.provider_id OR ee.provider_id IS NOT DISTINCT FROM qap.provider_id
 order by 1,3,2

-- Rows: 14,829 | Distinct Rows: 9,263 | Time: 4.1 secs
 SELECT DISTINCT ee.entry_exit_id, uat.tier_link, virt_field_name, answer_val, date_effective
 FROM qlik_answer_access qaa 
 JOIN tmp_relevant_ees ee ON (ee.client_id = qaa.client_id AND qaa.date_effective::DATE <= ee.entry_date::DATE)
 JOIN qlik_user_access_tier_view uat ON (uat.user_access_tier != 1)
 WHERE ee.provider_id = uat.provider_id and entry_exit_id = 731714
    OR (qaa.visibility_id IS NOT NULL 
        AND EXISTS (SELECT 1 FROM qlik_answer_vis_provider qap WHERE qap.visibility_id = qaa.visibility_id AND qap.provider_id = ee.provider_id))
 order by 1,3,2

/* Old way without explicit visibility */
-- Rows: 6,369
 SELECT ee.entry_exit_id, uat.tier_link, virt_field_name, answer_val, date_effective
 FROM qlik_answer_access qaa 
 JOIN tmp_relevant_ees ee ON (ee.client_id = qaa.client_id AND qaa.date_effective::DATE <= ee.entry_date::DATE)
 JOIN qlik_user_access_tier_view uat ON (ee.provider_id = uat.provider_id AND uat.user_access_tier != 1)
 where entry_exit_id = 731714
 order by 1,3,2


------- QA TESTS --------

-- Globally Open records that aren't any different than just the inherent vis way
select * from sp_client_answervisibility where client_answer_id in (
select answer_id
from (

 -- Records where Explicit that didn't make any difference using the new way
  SELECT ee.entry_exit_id, uat.tier_link, virt_field_name, qaa.answer_id, answer_val, date_effective
 FROM qlik_answer_access qaa
 JOIN tmp_relevant_ees ee ON (ee.client_id = qaa.client_id AND qaa.date_effective::DATE <= ee.entry_date::DATE)
 JOIN qlik_user_access_tier_view uat ON (ee.provider_id = uat.provider_id AND uat.user_access_tier != 1)
 WHERE entry_exit_id not in (
select entry_exit_id from (

-- Difference between including Explicit visibility checks
 SELECT DISTINCT ee.entry_exit_id, uat.tier_link, virt_field_name, qaa.answer_id, answer_val, date_effective
 FROM qlik_answer_access qaa
 JOIN tmp_relevant_ees ee ON (ee.client_id = qaa.client_id AND qaa.date_effective::DATE <= ee.entry_date::DATE)
 JOIN qlik_user_access_tier_view uat ON (uat.user_access_tier != 1)
 WHERE ee.provider_id = uat.provider_id 
    OR (qaa.visibility_id IS NOT NULL 
        AND EXISTS (SELECT 1 FROM qlik_answer_vis_provider qap WHERE qap.visibility_id = qaa.visibility_id AND qap.provider_id = ee.provider_id))
 EXCEPT
  -- Old way (Implicit Only)
  SELECT ee.entry_exit_id, uat.tier_link, virt_field_name, qaa.answer_id, answer_val, date_effective
 FROM qlik_answer_access qaa
 JOIN tmp_relevant_ees ee ON (ee.client_id = qaa.client_id AND qaa.date_effective::DATE <= ee.entry_date::DATE)
 JOIN qlik_user_access_tier_view uat ON (ee.provider_id = uat.provider_id AND uat.user_access_tier != 1)
 )t))t2)


---- Analysis Queries ------
select client_answer_id from sp_client_answervisibility where visibility_group_id != 0 and date_added > '2019-12-01'::date

select * from sp_entry_exit where entry_exit_id = 27186; -- provider 91 client 8216867 entry date "2010-07-11 01:00:00-05"
select * from da_question where virt_field_name = 'SVPPROFDOB'  -- question_id 893
select * from da_answer where client_id = 8216867 and question_id = 893; -- 1407198, 33062, 2363380
select * from sp_client_answervisibility where client_id = 8216867 AND client_answer_id in (1407198, 33062, 2363380); -- 33062, 1407198 have global open
select * from da_answer where answer_id in (33062, 1407198); 
-- provider id 91 | user 36 - date eff "2007-11-11 01:00:00-06" val_date "1967-12-22 01:00:00-06" | user 348 - date eff "2014-02-10 01:00:00-06" val_date "1967-12-22 13:00:00-06"

select * from sp_client_answervisibility where client_answer_id = 2963574

select * from qlik_answer_access where answer_id = 33062 -- 731714?


select * from qlik_user_access_tier_view where user_provider_id in (127, 18, 91);
select * from sp_provider_tree where provider_id in (127, 18, 91)

select * from sp_user where user_id in (910, 845, 813);
