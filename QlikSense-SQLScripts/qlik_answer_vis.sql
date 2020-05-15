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

CREATE TABLE IF NOT EXISTS public.qlik_answer_vis_provider(
  visibility_id integer,
  provider_id integer
);

CREATE OR REPLACE FUNCTION public.qlik_get_vis_link(
    allowvg integer[],
    denyvg integer[])
  RETURNS integer AS
$BODY$
        DECLARE
                _visibility_id INTEGER;
        BEGIN
            -- Function version db99999
            allowvg := (SELECT ARRAY(SELECT t FROM unnest($1) v(t) WHERE t IS NOT NULL ORDER BY 1));
            denyvg := (SELECT ARRAY(SELECT t FROM unnest($2) v(t) WHERE t IS NOT NULL ORDER BY 1));

            _visibility_id := (SELECT visibility_id FROM qlik_answer_vis_array WHERE allow_ids = allowvg AND deny_ids = denyvg);

            IF (_visibility_id IS NULL) THEN
                INSERT INTO qlik_answer_vis_array (allow_ids, deny_ids)
                VALUES (allowvg, denyvg)
                RETURNING visibility_id INTO _visibility_id;

                INSERT INTO qlik_answer_vis_provider (visibility_id, provider_id)
                SELECT _visibility_id, provider_id
                FROM (
                  SELECT provider_id
                  FROM sp_visibility_group_provider_tree vgpt 
                  WHERE vgpt.visibility_group_id = ANY(allowvg)
                  AND EXISTS (SELECT 1 FROM tmp_table_sec_non_support t WHERE vgpt.provider_id = t.provider_id)
                  EXCEPT
                  SELECT provider_id
                  FROM sp_visibility_group_provider_tree vgpt
                  WHERE vgpt.visibility_group_id = ANY(denyvg)
                ) p;
            END IF;

            RETURN _visibility_id;
        END;
        $BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

-- This gets set in multiple places so make sure we change it everywhere
CREATE OR REPLACE VIEW public.qlik_answer_questions AS 
SELECT question_id, virt_field_name, qt.code AS question_type_code
FROM da_question t JOIN da_question_type qt USING (question_type_id)
WHERE t.active
  AND t.parent_id IS NULL 
  AND qt.code IN ('lookup', 'yes_no', 'date', 'int', 'textbox', 'textarea', 'money', 'service_code')
  AND t.published = TRUE
  AND EXISTS(SELECT 1 FROM da_assessment_question aq JOIN da_assessment a USING (assessment_id) WHERE a.art_reportable_flag AND a.active AND aq.question_id = t.question_id AND a.code != 'SUPER_GLOBAL');


CREATE OR REPLACE FUNCTION qlik_build_answer_access(
    _delta_date character varying,
    _entry_exit_date character varying)
  RETURNS void AS
$BODY$
DECLARE
BEGIN
    DROP TABLE IF EXISTS qlik_answer_access;

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

    CREATE TABLE qlik_answer_access AS
    SELECT DISTINCT a.answer_id, q.question_type_code, q.virt_field_name, a.client_id, a.covered_by_roi, a.provider_id, a.date_effective, (i.answer_id IS NOT NULL) AS has_inherent_vis,  
    NULL::VARCHAR AS answer_val, NULL::INTEGER AS visibility_id
    FROM da_answer a
    JOIN qlik_answer_questions q USING (question_id)
    LEFT JOIN (
    -- Setting SA2 top answers
    SELECT answer_id
    FROM (select DISTINCT ON (client_id, question_id) answer_id, provider_id
          FROM da_answer a 
          WHERE a.active AND a.date_added > $1::DATE
          ORDER BY client_id, question_id, date_effective desc, answer_id desc) t
    -- Setting Admin top answers
    UNION
    SELECT answer_id
    FROM (select DISTINCT ON (client_id, question_id) answer_id, provider_id
          FROM da_answer a JOIN tmp_table_sec_aa t USING (provider_id)
          WHERE a.active AND a.date_added > $1::DATE
          ORDER BY client_id, question_id, date_effective desc, answer_id desc) t
    -- Setting CM top answers
    UNION
    SELECT answer_id
    FROM (select DISTINCT ON (client_id, question_id) answer_id, provider_id
          FROM da_answer a JOIN tmp_table_sec_cm t USING (provider_id)
          WHERE a.active AND a.date_added > $1::DATE
          ORDER BY client_id, question_id, date_effective desc, answer_id desc) t
    ) i ON (a.answer_id = i.answer_id) 

    WHERE a.active AND a.date_added > $1::DATE
    AND (i.answer_id IS NOT NULL OR covered_by_roi) -- Remove non-roi rows
    ORDER BY a.answer_id, q.question_type_code, q.virt_field_name, a.client_id, a.covered_by_roi, a.provider_id, a.date_effective, i.answer_id;

    -- Create primary key index
    ALTER TABLE qlik_answer_access ADD PRIMARY KEY (answer_id);

    -- Remove all records with no inherent or explicit visiblity
    DELETE FROM qlik_answer_access qaa 
    WHERE has_inherent_vis = FALSE 
      AND NOT EXISTS (SELECT 1 FROM sp_client_answervisibility v WHERE qaa.answer_id = v.client_answer_id);

    -- Remove any globally denies from the list
    DELETE FROM qlik_answer_access qaa 
    WHERE has_inherent_vis = FALSE 
      AND EXISTS (SELECT 1 FROM sp_client_answervisibility v WHERE qaa.answer_id = v.client_answer_id AND visibility_group_id = 0 AND visible = FALSE);

    -- Create global open visibility record up front to limit queries
    WITH global_open AS (
    SELECT qlik_get_vis_link(ARRAY[0], NULL) AS visibility_id
    )
    UPDATE qlik_answer_access qaa
    SET visibility_id = (SELECT g.visibility_id FROM global_open g)
    WHERE EXISTS (SELECT 1 FROM sp_client_answervisibility v WHERE qaa.answer_id = v.client_answer_id AND visibility_group_id = 0 AND visible);

    -- Now run Explicit after all the other rows are set
    UPDATE qlik_answer_access qaa
    SET visibility_id = (SELECT qlik_get_vis_link(array_agg(CASE WHEN cav.visible THEN cav.visibility_group_id ELSE NULL END), 
                                                  array_agg(CASE WHEN NOT cav.visible THEN cav.visibility_group_id ELSE NULL END))
                         FROM sp_client_answervisibility cav
                         WHERE cav.client_answer_id = qaa.answer_id 
                         GROUP BY client_answer_id)
    WHERE visibility_id IS NULL;

    -- Remove all unneeded rows
    DELETE FROM qlik_answer_access WHERE has_inherent_vis = FALSE AND visibility_id IS NULL;
    
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

SELECT qlik_build_answer_access('2015-01-01', '2015-01-01');

/* CLEAN UP */
DROP TABLE IF EXISTS tmp_table_sec_aa;
DROP TABLE IF EXISTS tmp_table_sec_cm;
DROP TABLE IF EXISTS tmp_table_sec_bypass;
DROP TABLE IF EXISTS tmp_table_sec_non_support;
DROP TABLE IF EXISTS tmp_relevant_ees;