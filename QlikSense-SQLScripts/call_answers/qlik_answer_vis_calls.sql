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

    /* ***************************************************** */
    /* ***** SETTING INHERENT AND EXPLICIT VISIBILITY ****** */
    /* ***************************************************** */

    CREATE TABLE qlik_call_answer_access AS
    SELECT DISTINCT a.call_answer_id, q.question_type_code, q.virt_field_name, a.client_id, a.covered_by_roi, a.provider_id, a.date_effective, (i.call_answer_id IS NOT NULL) AS has_inherent_vis,  
    NULL::VARCHAR AS answer_val, NULL::INTEGER AS visibility_id
    FROM call_answer a
    JOIN qlik_answer_questions q USING (question_id)
    LEFT JOIN (
    -- Check if answers are used by SA2, Admin, or CM Qlik users
    SELECT call_answer_id
    FROM (select DISTINCT call_answer_id
          FROM call_answer a 
          JOIN qlik_ee_user_access_tier_view uat USING (client_id)
          LEFT JOIN sp_entry_exit_review eer ON (uat.entry_exit_id = eer.entry_exit_id)
          WHERE a.active AND a.date_added > $1::DATE 
            AND a.date_effective::DATE <= GREATEST(uat.entry_date::DATE, uat.exit_date::DATE, eer.review_date::DATE)
            AND (exit_date IS NULL OR exit_date::DATE >= $2::DATE)) t
    ) i ON (a.call_answer_id = i.call_answer_id)

    WHERE a.active AND a.date_added > $1::DATE
    AND (i.call_answer_id IS NOT NULL OR covered_by_roi) -- Remove non-roi rows
    ORDER BY a.call_answer_id, q.question_type_code, q.virt_field_name, a.client_id, a.covered_by_roi, a.provider_id, a.date_effective, i.call_answer_id;

    -- Create primary key index
    ALTER TABLE qlik_call_answer_access ADD PRIMARY KEY (call_answer_id);

    -- Remove all records with no inherent or explicit visiblity
    DELETE FROM qlik_call_answer_access qaa 
    WHERE has_inherent_vis = FALSE 
      AND NOT EXISTS (SELECT 1 FROM sp_call_answervisibility v WHERE qaa.call_answer_id = v.call_answer_id AND v.visible);

    -- Mark records with only denies as globally denied since denied is the default state
    UPDATE qlik_answer_access qaa
    SET visibility_id = _global_deny_vis_id
    WHERE NOT(SELECT COALESCE(visible, FALSE) FROM sp_call_answervisibility cav WHERE cav.call_answer_id = qaa.call_answer_id ORDER BY visible DESC limit 1);

    -- Mark records with only allows and a global allow as just globally allowed to reduce queries
    UPDATE qlik_answer_access qaa
    SET visibility_id = _global_allow_vis_id
    WHERE visibility_id IS NULL AND
    EXISTS(SELECT 1 
           FROM sp_call_answervisibility cav 
           WHERE cav.call_answer_id = qaa.call_answer_id 
           GROUP BY call_answer_id 
           MIN(visible::integer) = 1 AND MIN(visibility_group_id) = 0);

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