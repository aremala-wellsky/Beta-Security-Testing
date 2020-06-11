CREATE OR REPLACE FUNCTION qlik_build_call_answer_pivot_views(
    _delta_date character varying,
    _call_record_date character varying,
    _types VARCHAR[])
  RETURNS void AS
$BODY$
DECLARE
    _type VARCHAR;
    _dsql TEXT;
    _question_query TEXT;
    _inner_query TEXT;
    _final_query TEXT;
    _call_limit VARCHAR;
BEGIN
    -- Version 20200611-1

    CREATE TEMP TABLE tmp_qlik_vis_provider AS
    SELECT visibility_id, vgpt.provider_id
    FROM qlik_answer_vis_array qav 
    JOIN sp_visibility_group_provider_tree vgpt ON (vgpt.visibility_group_id = ANY(allow_ids))
    JOIN (SELECT DISTINCT provider_id FROM qlik_user_access_tier_view WHERE user_access_tier != 1) up ON (vgpt.provider_id = up.provider_id)
    EXCEPT
    SELECT visibility_id, vgpt.provider_id
    FROM qlik_answer_vis_array qav 
    JOIN sp_visibility_group_provider_tree vgpt ON (vgpt.visibility_group_id = ANY(deny_ids))
    JOIN (SELECT DISTINCT provider_id FROM qlik_user_access_tier_view WHERE user_access_tier != 1) up ON (vgpt.provider_id = up.provider_id);

    _types := CASE WHEN ($3 IS NULL) THEN ARRAY['call', 'call_followup'] ELSE $3 END;

    FOREACH _type IN ARRAY _types LOOP
        _call_limit := CASE WHEN _type = 'call' THEN 'qaa.date_effective::DATE <= cr.start_date::DATE' ELSE 'cr.actual_followup_date IS NOT NULL AND qaa.date_effective::DATE <= cr.actual_followup_date::DATE' END;
    
        _question_query := 'SELECT DISTINCT virt_field_name FROM qlik_'||_type||'_answers 
        UNION SELECT DISTINCT virt_field_name FROM qlik_answer_access qaa ORDER BY 1';

        _inner_query := 'SELECT DISTINCT ON (tier_link, call_record_id, virt_field_name) tier_link||''''|''''||call_record_id AS sec_key, virt_field_name, answer_val
                 FROM (
                 -- Tier 1 - Top answers
                 SELECT DISTINCT cr.call_record_id, tier_link, virt_field_name, answer_val, date_effective 
                 FROM qlik_'||_type||'_answers qea
                 JOIN sp_call_record cr USING (call_record_id)
                 JOIN (SELECT DISTINCT tier_link, provider_id AS provider_creating_id FROM qlik_user_access_tier_view t WHERE t.user_access_tier = 1) uat USING (provider_creating_id)
                 WHERE end_date IS NULL OR end_date::DATE >= '''''||$2||'''''::DATE
                 UNION
                 -- Tier 2/3 - INHERITED
                 SELECT DISTINCT cr.call_record_id, uat.tier_link, virt_field_name, answer_val, date_effective
                 FROM qlik_call_answer_access qaa
                 JOIN sp_call_record cr ON (qaa.provider_id = cr.provider_creating_id AND qaa.call_record_id = cr.call_record_id AND '||_call_limit||')
                 JOIN (SELECT DISTINCT tier_link, provider_id AS provider_creating_id FROM qlik_user_access_tier_view t WHERE t.user_access_tier = 1) uat USING (provider_creating_id)
                 WHERE end_date IS NULL OR end_date::DATE >= '''''||$2||'''''::DATE 
                 UNION
                 -- Tier 2/3 - EXPLICIT
                 SELECT DISTINCT cr.call_record_id, uat.user_access_tier||''''|''''||qavp.provider_id AS tier_link, virt_field_name, answer_val, date_effective
                 FROM qlik_call_answer_access qaa 
                 JOIN sp_call_record cr ON (qaa.provider_id = cr.provider_creating_id AND qaa.call_record_id = cr.call_record_id AND '||_call_limit||')
                 JOIN tmp_qlik_vis_provider qavp USING (visibility_id)
                 CROSS JOIN (SELECT DISTINCT user_access_tier FROM qlik_user_access_tier_view WHERE user_access_tier != 1) uat
                 WHERE end_date IS NULL OR end_date::DATE >= '''''||$2||'''''::DATE) t
                 ORDER BY call_record_id, tier_link, virt_field_name, date_effective DESC';

        _dsql := 'SELECT FORMAT(
        $$
          SELECT * FROM crosstab('''||_inner_query||''', '''||_question_query||''')
        AS
        (
            sec_key VARCHAR,
            %s
        )
        $$,
        string_agg(FORMAT(''%I %s'', upper(virt_field_name)||''_'||_type||''', ''TEXT''), '', '' ORDER BY virt_field_name)
    )
    FROM (
        '||_question_query||'
    )
        AS t';

        RAISE NOTICE 'Creating the pivot query %',clock_timestamp();
        EXECUTE _dsql INTO _final_query;
        RAISE NOTICE 'Finished creating pivot query %: %', _type, clock_timestamp();

        EXECUTE 'DROP MATERIALIZED VIEW IF EXISTS qlik_'||_type||'_answer_pivot_view';
        EXECUTE 'CREATE MATERIALIZED VIEW qlik_'||_type||'_answer_pivot_view AS '||_final_query;
        RAISE NOTICE 'Finished creating pivot view %: %', _type, clock_timestamp();
        
        EXECUTE 'ALTER TABLE qlik_'||_type||'_answer_pivot_view OWNER TO sp5user';
    END LOOP;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

-- select qlik_build_call_answer_pivot_views('2015-01-01', '2015-01-01', NULL);
-- select * from qlik_entry_answer_pivot_view;