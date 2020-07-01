CREATE OR REPLACE FUNCTION qlik_build_answer_pivot_views(
    _delta_date character varying,
    _entry_exit_date character varying,
    _types character varying[])
  RETURNS void AS
$BODY$
DECLARE
    _type VARCHAR;
    _dsql TEXT;
    _question_query TEXT;
    _inner_query TEXT;
    _final_query TEXT;
    _ee_limit VARCHAR;
    _has_data BOOLEAN;
BEGIN
    -- Version 20200701-1

    _types := CASE WHEN ($3 IS NULL) THEN ARRAY['entry', 'exit'] ELSE $3 END;

    DROP TABLE IF EXISTS tmp_relevant_ees;
    DROP TABLE IF EXISTS tmp_qlik_vis_provider;

    CREATE TEMP TABLE tmp_relevant_ees AS
    SELECT entry_exit_id, tier_link, user_access_tier, client_id, entry_date, exit_date, provider_id, covered_by_roi, NULL::integer visibility_id
    FROM qlik_ee_user_access_tier_view uat
    WHERE uat.user_access_tier != 1 AND (exit_date IS NULL OR exit_date::DATE >= $2::DATE);

    UPDATE tmp_relevant_ees qaa
    SET visibility_id = (SELECT qlik_get_vis_link(array_agg(CASE WHEN eev.visible THEN eev.visibility_group_id ELSE NULL END), 
                                                  array_agg(CASE WHEN NOT eev.visible THEN eev.visibility_group_id ELSE NULL END))
                         FROM sp_entry_exitvisibility eev
                         WHERE eev.entry_exit_id = qaa.entry_exit_id 
                         GROUP BY entry_exit_id);

    -- Set visibility with only denies to null
    UPDATE tmp_relevant_ees trc
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
        _ee_limit := CASE WHEN _type = 'exit' THEN '(ee.exit_date IS NULL OR qaa.date_effective::DATE <= ee.exit_date::DATE)' ELSE 'qaa.date_effective::DATE <= ee.entry_date::DATE' END;
    
        _question_query := 'SELECT DISTINCT virt_field_name FROM qlik_'||_type||'_answers 
        UNION SELECT DISTINCT virt_field_name FROM qlik_answer_access qaa ORDER BY 1';

        _inner_query := 'SELECT DISTINCT ON (tier_link, entry_exit_id, virt_field_name) tier_link||''''|''''||entry_exit_id AS sec_key, virt_field_name, answer_val
                 FROM (
                 -- Tier 1 - Top answers
                 SELECT DISTINCT ee.entry_exit_id, tier_link, virt_field_name, answer_val, date_effective 
                 FROM qlik_'||_type||'_answers qea
                 JOIN (SELECT DISTINCT tier_link, entry_exit_id FROM qlik_ee_user_access_tier_view t WHERE t.user_access_tier = 1) ee USING (entry_exit_id)
                 UNION
                 -- Tier 2/3 Inherited EEs and Inherited/Explicit answers
                 SELECT DISTINCT ee.entry_exit_id, ee.tier_link, virt_field_name, answer_val, date_effective
                 FROM qlik_answer_access qaa 
                 JOIN tmp_relevant_ees ee ON (ee.client_id = qaa.client_id AND '||_ee_limit||')
                 WHERE ee.provider_id = qaa.provider_id
                   -- Inherited/Explicit answers
                   OR (qaa.visibility_id IS NOT NULL 
                       AND EXISTS (SELECT 1 FROM tmp_qlik_vis_provider qap WHERE qap.visibility_id = qaa.visibility_id AND qap.provider_id = qaa.provider_id))
                 UNION
                 -- Tier 2/3 Explicit EEs 
                 SELECT DISTINCT ee.entry_exit_id, (user_access_tier||''''|''''||tvp.provider_id) AS tier_link, virt_field_name, answer_val, date_effective
                 FROM qlik_answer_access qaa 
                 JOIN tmp_relevant_ees ee ON (ee.client_id = qaa.client_id AND '||_ee_limit||')
                 JOIN tmp_qlik_vis_provider tvp ON (ee.visibility_id = tvp.visibility_id AND ee.provider_id != tvp.provider_id)
                 WHERE ee.visibility_id IS NOT NULL AND ee.covered_by_roi
                   -- Inherited/Explicit answers
                   AND (tvp.provider_id = qaa.provider_id 
                     OR (qaa.visibility_id IS NOT NULL AND qaa.covered_by_roi
                         AND EXISTS (SELECT 1 
                                     FROM tmp_qlik_vis_provider qap 
                                     WHERE qap.visibility_id = qaa.visibility_id 
                                       AND qap.provider_id = qaa.provider_id))
                   )
                   )
                 ) t
                 ORDER BY entry_exit_id, tier_link, virt_field_name, date_effective DESC';

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

-- select qlik_build_answer_pivot_views('2015-01-01', '2015-01-01', ARRAY['entry']);
-- select * from qlik_entry_answer_pivot_view;