/* **********************************************************************
************************ CREATING USER TABLES *************************
********************************************************************** */

/* CLEAN UP */
DROP TABLE IF EXISTS tmp_table_sec_aa;
DROP TABLE IF EXISTS tmp_table_sec_cm;
DROP TABLE IF EXISTS tmp_table_sec_bypass;
DROP TABLE IF EXISTS tmp_table_sec_non_support;


/* **************************************************************************** */
/* ************************ SETUP EXPLICIT VISIBILITY ************************* */
/* **************************************************************************** */

-- Assume qlik_answer_vis_array already exists and was created in qlik_answer_vis.sql so we don't step on each other
-- Assume qlik_answer_vis_provider already exists and was set in qlik_answer_vis.sql so we don't step on each other
-- Assume qlik_get_vis_link already exists and was created in qlik_answer_vis.sql so we don't step on each other
-- Assume qlik_answer_questions already exists and was set in qlik_answer_vis.sql so we don't step on each other

CREATE OR REPLACE FUNCTION qlik_build_call_answer_access(
    _delta_date character varying,
    _call_record_date character varying)
  RETURNS void AS
$BODY$
DECLARE
BEGIN
    DROP TABLE IF EXISTS qlik_call_answer_access;

    /* GET QLIK USERS */
    CREATE TEMP TABLE tmp_table_sec_aa AS
    SELECT DISTINCT provider_id
    FROM qlik_user_access_tier_view uap
    WHERE uap.user_access_tier = 2;

    CREATE TEMP TABLE tmp_table_sec_cm AS
    SELECT DISTINCT provider_id
    FROM qlik_user_access_tier_view
    WHERE user_access_tier = 3;

    CREATE TEMP TABLE tmp_table_sec_non_support AS
    SELECT DISTINCT provider_id FROM tmp_table_sec_aa
    UNION
    SELECT DISTINCT provider_id FROM tmp_table_sec_cm;

    /* ***************************************************** */
    /* ***** SETTING INHERENT AND EXPLICIT VISIBILITY ****** */
    /* ***************************************************** */

    CREATE TABLE qlik_call_answer_access AS
    SELECT DISTINCT a.call_answer_id, q.question_type_code, q.virt_field_name, a.client_id, a.covered_by_roi, a.provider_id, a.date_effective, (i.call_answer_id IS NOT NULL) AS has_inherent_vis,  
    NULL::VARCHAR AS answer_val, NULL::INTEGER AS visibility_id
    FROM call_answer a
    JOIN qlik_answer_questions q USING (question_id)
    LEFT JOIN (
    -- Setting SA2 top answers
    SELECT call_answer_id
    FROM (select DISTINCT ON (client_id, question_id) call_answer_id, provider_id
          FROM call_answer a 
          WHERE a.active AND a.date_added > $1::DATE
          ORDER BY client_id, question_id, date_effective desc, call_answer_id desc) t
    -- Setting Admin top answers
    UNION
    SELECT call_answer_id
    FROM (select DISTINCT ON (client_id, question_id) call_answer_id, provider_id
          FROM call_answer a JOIN tmp_table_sec_aa t USING (provider_id)
          WHERE a.active AND a.date_added > $1::DATE
          ORDER BY client_id, question_id, date_effective desc, call_answer_id desc) t
    -- Setting CM top answers
    UNION
    SELECT call_answer_id
    FROM (select DISTINCT ON (client_id, question_id) call_answer_id, provider_id
          FROM call_answer a JOIN tmp_table_sec_cm t USING (provider_id)
          WHERE a.active AND a.date_added > $1::DATE
          ORDER BY client_id, question_id, date_effective desc, call_answer_id desc) t
    ) i ON (a.call_answer_id = i.call_answer_id) 

    WHERE a.active AND a.date_added > $1::DATE
    AND (i.call_answer_id IS NOT NULL OR covered_by_roi) -- Remove non-roi rows
    ORDER BY a.call_answer_id, q.question_type_code, q.virt_field_name, a.client_id, a.covered_by_roi, a.provider_id, a.date_effective, i.call_answer_id;

    -- Create primary key index
    ALTER TABLE qlik_call_answer_access ADD PRIMARY KEY (call_answer_id);

    -- Remove all records with no inherent or explicit visiblity
    DELETE FROM qlik_call_answer_access qaa 
    WHERE has_inherent_vis = FALSE 
      AND NOT EXISTS (SELECT 1 FROM sp_call_answervisibility v WHERE qaa.call_answer_id = v.call_answer_id);

    -- Remove any globally denies from the list
    DELETE FROM qlik_call_answer_access qaa 
    WHERE has_inherent_vis = FALSE 
      AND EXISTS (SELECT 1 FROM sp_call_answervisibility v WHERE qaa.call_answer_id = v.call_answer_id AND visibility_group_id = 0 AND visible = FALSE);

    -- Create global open visibility record up front to limit queries
    WITH global_open AS (
    SELECT qlik_get_vis_link(ARRAY[0], NULL) AS visibility_id
    )
    UPDATE qlik_call_answer_access qaa
    SET visibility_id = (SELECT g.visibility_id FROM global_open g)
    WHERE EXISTS (SELECT 1 FROM sp_call_answervisibility v WHERE qaa.call_answer_id = v.call_answer_id AND visibility_group_id = 0 AND visible);

    -- Now run Explicit after all the other rows are set
    UPDATE qlik_call_answer_access qaa
    SET visibility_id = (SELECT qlik_get_vis_link(array_agg(CASE WHEN cav.visible THEN cav.visibility_group_id ELSE NULL END), 
                                                  array_agg(CASE WHEN NOT cav.visible THEN cav.visibility_group_id ELSE NULL END))
                         FROM sp_call_answervisibility cav
                         WHERE cav.call_answer_id = qaa.call_answer_id 
                         GROUP BY call_answer_id)
    WHERE visibility_id IS NULL;

    -- Remove all unneeded rows
    DELETE FROM qlik_call_answer_access WHERE has_inherent_vis = FALSE AND visibility_id IS NULL;
    
    -- Set answer_val now that we've reduced the rows to update
    UPDATE qlik_call_answer_access q
    SET answer_val = (
        CASE WHEN q.question_type_code = 'lookup' THEN plv(a.val_int)::VARCHAR
        WHEN q.question_type_code = 'yes_no' THEN yn(a.val_int)::VARCHAR
        WHEN q.question_type_code = 'date' THEN TO_CHAR((a.val_date)::TIMESTAMP::DATE,'MM/dd/YYYY')
        WHEN q.question_type_code = 'int' THEN a.val_int::VARCHAR
        WHEN q.question_type_code = 'textbox' THEN substring(a.val_textfield::VARCHAR from 1 for 200)
        WHEN q.question_type_code = 'textarea' THEN substring(a.val_textfield::VARCHAR from 1 for 200)
        WHEN q.question_type_code = 'money' THEN a.val_float::VARCHAR
        WHEN q.question_type_code = 'service_code' THEN a.val_int::VARCHAR
        ELSE '' END)
    FROM call_answer a
    WHERE q.call_answer_id = a.call_answer_id;
END;
$BODY$
LANGUAGE plpgsql VOLATILE
COST 100;

SELECT qlik_build_call_answer_access('2015-01-01', '2015-01-01');

/* CLEAN UP */
DROP TABLE IF EXISTS tmp_table_sec_aa;
DROP TABLE IF EXISTS tmp_table_sec_cm;
DROP TABLE IF EXISTS tmp_table_sec_bypass;
DROP TABLE IF EXISTS tmp_table_sec_non_support;