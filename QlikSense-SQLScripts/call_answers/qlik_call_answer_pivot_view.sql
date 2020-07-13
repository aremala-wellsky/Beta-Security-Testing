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
    _has_data BOOLEAN;
BEGIN
    -- Version 20200701-1

    DROP TABLE IF EXISTS tmp_relevant_calls;
    DROP TABLE IF EXISTS tmp_qlik_vis_provider;

    _types := CASE WHEN ($3 IS NULL) THEN ARRAY['call', 'call_followup'] ELSE $3 END;

    CREATE TEMP TABLE tmp_relevant_calls AS
    SELECT call_record_id, tier_link, user_access_tier, client_id, start_date, actual_followup_date, provider_creating_id, covered_by_roi, NULL::integer visibility_id
    FROM sp_call_record cr
    JOIN qlik_user_access_tier_view AS sec ON (cr.provider_creating_id = sec.provider_id)
    WHERE sec.user_access_tier != 1 AND cr.active AND start_date::DATE >= $2::DATE;

    UPDATE tmp_relevant_calls trc
    SET visibility_id = (SELECT qlik_get_vis_link(array_agg(CASE WHEN crv.visible THEN crv.visibility_group_id ELSE NULL END), 
                                                  array_agg(CASE WHEN NOT crv.visible THEN crv.visibility_group_id ELSE NULL END))
                         FROM sp_callrecordvisibility crv
                         WHERE crv.callrecord_id = trc.call_record_id 
                         GROUP BY callrecord_id);
                      
    -- Set visibility with only denies to null
    UPDATE tmp_relevant_calls trc
    SET visibility_id = NULL
    WHERE EXISTS (SELECT 1 
                  FROM qlik_answer_vis_array va 
                  WHERE trc.visibility_id = va.visibility_id 
                    AND (allow_ids IS NULL OR array_length(allow_ids, 1) IS NULL));

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
                 -- Tier 2/3 Inherited Call Records
                 SELECT DISTINCT cr.call_record_id, cr.tier_link, virt_field_name, answer_val, date_effective
                 FROM qlik_call_answer_access qaa
                 JOIN tmp_relevant_calls cr ON (qaa.call_record_id = cr.call_record_id AND '||_call_limit||')
                 WHERE cr.provider_creating_id = qaa.provider_id
                   -- Inherited/Explicit answers
                   OR (qaa.visibility_id IS NOT NULL 
                       AND EXISTS (SELECT 1 FROM tmp_qlik_vis_provider qap WHERE qap.visibility_id = qaa.visibility_id AND qap.provider_id = qaa.provider_id))
                 UNION
                 -- Tier 2/3 Explicit Call Records
                 SELECT DISTINCT cr.call_record_id, (user_access_tier||''''|''''||tvp.provider_id) AS tier_link, virt_field_name, answer_val, date_effective
                 FROM qlik_call_answer_access qaa 
                 JOIN tmp_relevant_calls cr ON (cr.call_record_id = qaa.call_record_id AND '||_call_limit||')
                 JOIN tmp_qlik_vis_provider tvp ON (cr.visibility_id = tvp.visibility_id AND cr.provider_creating_id != tvp.provider_id) -- Creating ID handled with Inherent above
                 WHERE cr.visibility_id IS NOT NULL AND cr.covered_by_roi 
                 AND (tvp.provider_id = qaa.provider_id
                   -- Inherited/Explicit answers
                   OR (qaa.visibility_id IS NOT NULL AND qaa.covered_by_roi
                       AND EXISTS (SELECT 1 
                                   FROM tmp_qlik_vis_provider qap 
                                   WHERE qap.visibility_id = qaa.visibility_id 
                                     AND qap.provider_id = qaa.provider_id)))
                 ) t
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
        string_agg(FORMAT(''%I %s'', upper(virt_field_name)||''_'||(CASE WHEN _type = 'call_followup' THEN 'callfollow' ELSE _type END)||''', ''TEXT''), '', '' ORDER BY virt_field_name)
    )
    FROM (
        '||_question_query||'
    )
        AS t';

        EXECUTE 'SELECT EXISTS('||_question_query||') AS has_data' INTO _has_data;
        IF _has_data IS DISTINCT FROM TRUE THEN
            _final_query := 'SELECT NULL::VARCHAR AS sec_key LIMIT 0';
        ELSE
            RAISE NOTICE 'Creating the pivot query %',clock_timestamp();
            EXECUTE _dsql INTO _final_query;
            RAISE NOTICE 'Finished creating pivot query %: %', _type, clock_timestamp();
        END IF;

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
-- select * from qlik_call_answer_pivot_view WHERE split_part(sec_key, '|', 3) = '43027';