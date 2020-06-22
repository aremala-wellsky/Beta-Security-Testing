CREATE OR REPLACE FUNCTION qlik_build_review_answer_pivot_table(
    _delta_date character varying,
    _entry_exit_date character varying)
  RETURNS void AS
$BODY$
DECLARE
    _dsql TEXT;
    _question_query TEXT;
    _inner_query TEXT;
    _final_query TEXT;
    _has_data BOOLEAN;
BEGIN
    -- Version 20200622-1

    DROP TABLE IF EXISTS tmp_relevant_ees;
    DROP TABLE IF EXISTS tmp_qlik_vis_provider;

    CREATE TEMP TABLE tmp_relevant_ee_reviews AS
    SELECT entry_exit_review_id, entry_exit_id, tier_link, user_access_tier, client_id, point_in_time_type_id, review_type_id, 
           review_date, uat.provider_id, NULL::integer visibility_id
    FROM sp_entry_exit_review teer JOIN qlik_ee_user_access_tier_view uat USING (entry_exit_id)
    WHERE teer.active AND uat.user_access_tier != 1 AND (exit_date IS NULL OR exit_date::DATE >= $2::DATE);

    UPDATE tmp_relevant_ee_reviews qaa
    SET visibility_id = (SELECT qlik_get_vis_link(array_agg(CASE WHEN eev.visible THEN eev.visibility_group_id ELSE NULL END), 
                                                  array_agg(CASE WHEN NOT eev.visible THEN eev.visibility_group_id ELSE NULL END))
                         FROM sp_entry_exitvisibility eev
                         WHERE eev.entry_exit_id = qaa.entry_exit_id 
                         GROUP BY entry_exit_id);

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

        _question_query := 'SELECT DISTINCT virt_field_name FROM qlik_review_answers 
        UNION SELECT DISTINCT virt_field_name FROM qlik_answer_access qaa ORDER BY 1';

        _inner_query := 'SELECT DISTINCT ON (entry_exit_review_id, virt_field_name) entry_exit_review_id, virt_field_name, answer_val '||
                'FROM (
                 -- Tier 1 - Top answers
                 SELECT DISTINCT entry_exit_review_id, virt_field_name, answer_val, date_effective 
                 FROM qlik_review_answers qea 
                 WHERE qea.exit_date IS NULL OR qea.exit_date::DATE >= '''''||$2||'''''::DATE
                 UNION
                 -- Tier 2/3 Inherited and Explicit
                 SELECT DISTINCT ON (entry_exit_review_id, virt_field_name) entry_exit_review_id, virt_field_name, answer_val, date_effective
                 FROM qlik_answer_access qaa 
                 JOIN tmp_relevant_ee_reviews ee ON (ee.client_id = qaa.client_id AND qaa.date_effective::DATE <= ee.review_date::DATE)
                 WHERE ee.provider_id = qaa.provider_id 
                   -- Handle Inherited/Explicit answers here and EE visibility afterwards
                   OR (qaa.visibility_id IS NOT NULL 
                       AND EXISTS (SELECT 1 FROM tmp_qlik_vis_provider qap WHERE qap.visibility_id = qaa.visibility_id AND qap.provider_id = qaa.provider_id))
                 ) t
                 ORDER BY entry_exit_review_id, virt_field_name, date_effective DESC';

        _dsql := 'SELECT FORMAT(
        $$
          SELECT * FROM crosstab('''||_inner_query||''', '''||_question_query||''')
        AS
        (
            entry_exit_review_id INTEGER, %s
        )
        $$,
        string_agg(FORMAT(''%I %s'', upper(virt_field_name)||''_review'', ''TEXT''), '', '' ORDER BY virt_field_name)
    )
    FROM (
        '||_question_query||'
    )
        AS t';

    EXECUTE 'SELECT EXISTS('||_question_query||') AS has_data' INTO _has_data;
    IF _has_data IS DISTINCT FROM TRUE THEN
        _final_query := 'SELECT NULL::INTEGER AS entry_exit_review_id LIMIT 0';
    ELSE
        RAISE NOTICE 'Creating the pivot query %',clock_timestamp();
        EXECUTE _dsql INTO _final_query;
        RAISE NOTICE 'Finished creating pivot query: %', clock_timestamp();
    END IF;

    -- We have to create a separate table to define the column structure for the crosstab
    DROP TABLE IF EXISTS tmp_ee_review_crosstab;
    EXECUTE 'CREATE TEMP TABLE tmp_ee_review_crosstab AS '||_final_query;

    -- Create the reviews pivot view like this since we have additional data needed besides 
    -- the sec_key and cross tab doesn't like us adding multiple columns to it
    DROP TABLE IF EXISTS qlik_review_answer_pivot;
    CREATE TABLE qlik_review_answer_pivot AS
    SELECT tier_link||'|'||eer.entry_exit_id AS ee_sec_key, tier_link||'|'||eer.entry_exit_review_id AS sec_key,
           plv(eer.point_in_time_type_id) as entry_exit_review_pit_type, 
           plv(eer.review_type_id) AS entry_exit_review_type, 
           eer.review_date::DATE AS entry_exit_review_date, t.*
    FROM tmp_ee_review_crosstab t 
    --Handling Inherited EE visibility
    JOIN tmp_relevant_ee_reviews eer USING (entry_exit_review_id)
    UNION
    --Handling Explicit EE visibility
    SELECT user_access_tier||'|'||tvp.provider_id||'|'||eer.entry_exit_id AS ee_sec_key, user_access_tier||'|'||tvp.provider_id||'|'||eer.entry_exit_review_id AS sec_key,
           plv(eer.point_in_time_type_id) as entry_exit_review_pit_type, 
           plv(eer.review_type_id) AS entry_exit_review_type, 
           eer.review_date::DATE AS entry_exit_review_date, t.*
    FROM tmp_ee_review_crosstab t 
    JOIN tmp_relevant_ee_reviews eer USING (entry_exit_review_id)
    JOIN tmp_qlik_vis_provider tvp ON (eer.visibility_id = tvp.visibility_id AND eer.provider_id != tvp.provider_id)
    WHERE eer.visibility_id IS NOT NULL;

    RAISE NOTICE 'Finished creating pivot table: %', clock_timestamp();

    ALTER TABLE qlik_review_answer_pivot OWNER TO sp5user;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

-- select qlik_build_review_answers_table('2015-01-01', '2015-01-01');
-- select qlik_build_review_answer_pivot_table('2015-01-01', '2015-01-01');