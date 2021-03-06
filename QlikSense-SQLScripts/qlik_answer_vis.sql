/* **********************************************************************
************************ CREATING USER TABLES *************************
********************************************************************** */

/* CLEAN UP */
DROP TABLE IF EXISTS tmp_table_sec_aa;
DROP TABLE IF EXISTS tmp_table_sec_cm;
DROP TABLE IF EXISTS tmp_table_sec_bypass;
DROP TABLE IF EXISTS tmp_table_sec_non_support;
DROP TABLE IF EXISTS tmp_relevant_ees;


/* **************************************************************************** */
/* ************************ SETUP EXPLICIT VISIBILITY ************************* */
/* **************************************************************************** */


-- Gather Data
CREATE TABLE IF NOT EXISTS public.qlik_answer_vis_array(
  visibility_id serial PRIMARY KEY,
  allow_ids integer[],
  deny_ids integer[]
);

ALTER TABLE qlik_answer_vis_array OWNER TO sp5user;

CREATE OR REPLACE FUNCTION public.qlik_get_vis_link(
    allowvg integer[],
    denyvg integer[])
  RETURNS integer AS
$BODY$
        DECLARE
                _visibility_id INTEGER;
        BEGIN
            -- Version 20200602-1
            allowvg := (SELECT ARRAY(SELECT t FROM unnest($1) v(t) WHERE t IS NOT NULL ORDER BY 1));
            denyvg := (SELECT ARRAY(SELECT t FROM unnest($2) v(t) WHERE t IS NOT NULL ORDER BY 1));

            _visibility_id := (SELECT visibility_id FROM qlik_answer_vis_array WHERE allow_ids = allowvg AND deny_ids = denyvg);

            IF (_visibility_id IS NULL) THEN
                INSERT INTO qlik_answer_vis_array (allow_ids, deny_ids)
                VALUES (allowvg, denyvg)
                RETURNING visibility_id INTO _visibility_id;
            END IF;

            RETURN _visibility_id;
        END;
        $BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
  
ALTER FUNCTION qlik_get_vis_link (integer[], integer[]) OWNER TO sp5user;

-- This gets set in multiple places so make sure we change it everywhere
CREATE OR REPLACE VIEW public.qlik_answer_questions AS 
SELECT question_id, virt_field_name, qt.code AS question_type_code
FROM da_question t JOIN da_question_type qt USING (question_type_id)
WHERE t.active
  AND t.parent_id IS NULL 
  AND qt.code IN ('lookup', 'yes_no', 'date', 'int', 'textbox', 'textarea', 'money', 'service_code')
  AND t.published = TRUE
  AND EXISTS(SELECT 1 FROM da_assessment_question aq JOIN da_assessment a USING (assessment_id) WHERE a.art_reportable_flag AND a.active AND aq.question_id = t.question_id AND a.code != 'SUPER_GLOBAL');

ALTER TABLE qlik_answer_questions OWNER TO sp5user;

CREATE OR REPLACE FUNCTION qlik_build_answer_access(
    _delta_date character varying,
    _entry_exit_date character varying)
  RETURNS void AS
$BODY$
DECLARE
    _global_deny_vis_id INTEGER := (SELECT qlik_get_vis_link(NULL, ARRAY[0]));
    _global_allow_vis_id INTEGER := (SELECT qlik_get_vis_link(ARRAY[0], NULL));
BEGIN
    -- Version 20200602-1

    DROP TABLE IF EXISTS qlik_answer_access;

    /* ***************************************************** */
    /* ***** SETTING INHERENT AND EXPLICIT VISIBILITY ****** */
    /* ***************************************************** */

    CREATE TABLE qlik_answer_access AS
    SELECT DISTINCT a.answer_id, q.question_type_code, q.virt_field_name, a.client_id, a.covered_by_roi, a.provider_id, a.date_effective, (i.answer_id IS NOT NULL) AS has_inherent_vis,  
    NULL::VARCHAR AS answer_val, NULL::INTEGER AS visibility_id
    FROM da_answer a
    JOIN qlik_answer_questions q USING (question_id)
    LEFT JOIN (
    -- Check if answers are used by SA2, Admin, or CM Qlik users
    SELECT answer_id
    FROM (select DISTINCT answer_id
          FROM da_answer a 
          JOIN qlik_ee_user_access_tier_view uat USING (client_id)
          LEFT JOIN sp_entry_exit_review eer ON (uat.entry_exit_id = eer.entry_exit_id)
          WHERE a.active AND a.date_added > $1::DATE AND uat.user_access_tier != 1
            AND a.date_effective::DATE <= GREATEST(uat.entry_date::DATE, uat.exit_date::DATE, eer.review_date::DATE)
            AND (exit_date IS NULL OR exit_date::DATE >= $2::DATE)) t
    ) i ON (a.answer_id = i.answer_id) 

    WHERE a.active AND a.date_added > $1::DATE
    AND (i.answer_id IS NOT NULL OR covered_by_roi) -- Remove non-roi rows
    ORDER BY 1,2,3,4,5,6,7,8,9,10;

    -- Create primary key index
    ALTER TABLE qlik_answer_access ADD PRIMARY KEY (answer_id);

    -- Remove all records with no inherent or explicit visiblity
    DELETE FROM qlik_answer_access qaa 
    WHERE has_inherent_vis = FALSE 
      AND NOT EXISTS (SELECT 1 FROM sp_client_answervisibility v WHERE qaa.answer_id = v.client_answer_id AND v.visible);
    
    -- Mark records with only denies as globally denied since denied is the default state
    UPDATE qlik_answer_access qaa
    SET visibility_id = _global_deny_vis_id
    WHERE NOT(SELECT COALESCE(visible, FALSE) FROM sp_client_answervisibility cav WHERE cav.client_answer_id = qaa.answer_id ORDER BY visible DESC limit 1);

    -- Mark records with only allows and a global allow as just globally allowed to reduce queries
    UPDATE qlik_answer_access qaa
    SET visibility_id = _global_allow_vis_id
    WHERE visibility_id IS NULL AND
    EXISTS(SELECT 1 
           FROM sp_client_answervisibility cav 
           WHERE cav.client_answer_id = qaa.answer_id 
           GROUP BY answer_id 
           HAVING MIN(visible::integer) = 1 AND MIN(visibility_group_id) = 0);

    -- Now run Explicit after all the other rows are set
    UPDATE qlik_answer_access qaa
    SET visibility_id = (SELECT qlik_get_vis_link(array_agg(CASE WHEN cav.visible THEN cav.visibility_group_id ELSE NULL END), 
                                                  array_agg(CASE WHEN NOT cav.visible THEN cav.visibility_group_id ELSE NULL END))
                         FROM sp_client_answervisibility cav
                         WHERE cav.client_answer_id = qaa.answer_id 
                         GROUP BY client_answer_id)
    WHERE visibility_id IS NULL;

    -- Remove all unneeded rows
    DELETE FROM qlik_answer_access WHERE has_inherent_vis = FALSE AND (visibility_id IS NULL OR visibility_id = _global_deny_vis_id);
    
    -- Set answer_val now that we've reduced the rows to update
    UPDATE qlik_answer_access q
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
    FROM da_answer a
    WHERE q.answer_id = a.answer_id;
END;
$BODY$
LANGUAGE plpgsql VOLATILE
COST 100;

ALTER FUNCTION qlik_build_answer_access (character varying, character varying) OWNER TO sp5user;

-- SELECT qlik_build_answer_access('2015-01-01', '2015-01-01');

/* CLEAN UP */
DROP TABLE IF EXISTS tmp_table_sec_aa;
DROP TABLE IF EXISTS tmp_table_sec_cm;
DROP TABLE IF EXISTS tmp_table_sec_bypass;
DROP TABLE IF EXISTS tmp_table_sec_non_support;
DROP TABLE IF EXISTS tmp_relevant_ees;