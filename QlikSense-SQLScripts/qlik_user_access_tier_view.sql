-- DROP VIEW IF EXISTS qlik_user_access_tier_view;
 
CREATE OR REPLACE VIEW qlik_user_access_tier_view AS 
WITH active_art_users AS (
  SELECT DISTINCT user_id
  FROM sp_boxi_license_allocation bla JOIN sp_boxi_license bl ON bl.license_id = bla.license_id JOIN boxi_license_type blt ON blt.license_type_id = bl.license_type_id
  WHERE bla.active AND bl.active AND blt.name::text = 'ART-AR'::text
), 
tier1_roles AS (
  SELECT DISTINCT role_id FROM sp_action_in_role ar JOIN action a ON ar.action_id = a.action_id WHERE a.name = 'VISIBILITY_BYPASSSECURITY'
), 
tier2_roles AS (
  SELECT DISTINCT role_id FROM sp_action_in_role ar JOIN action a USING (action_id) WHERE a.name = 'VISIBILITY_BYPASSSECURITY_TREE'
), 
users_with_client_perm AS (
  SELECT DISTINCT user_id, u.provider_id, u.allow_clientpoint, p.enable_clientpoint
  FROM sp_user u 
    JOIN sp_provider p USING (provider_id) 
    JOIN (SELECT DISTINCT role_id, r.name FROM sp_action_in_role ar JOIN sp_role r USING (role_id) JOIN action a USING (action_id) WHERE a.name = 'CLIENT_CLIENT_VIEW') r USING (role_id)
  WHERE u.active AND p.active AND (
       (r.name ILIKE 'READ_ONLY%' AND (p.enable_clientpoint OR p.activity_flag OR p.callcenter_flag))
    OR (p.enable_clientpoint AND u.allow_clientpoint)
    OR EXISTS (SELECT 1
               FROM sp_setting 
               WHERE val_bool IS NOT DISTINCT FROM TRUE 
                 AND ((name = 'callcenter_211module' AND p.callcenter_flag AND u.allow_callpoint) 
                   OR (name = 'activitypoint_module' AND p.activity_flag AND u.allow_activitypoint)))
  )
), 
recent_users AS (
  SELECT DISTINCT u.user_id, u.role_id, u.provider_id AS user_provider_id
  FROM sp_user u 
    JOIN active_art_users bl ON (bl.user_id = u.user_id)
    JOIN users_with_client_perm c ON (c.user_id = u.user_id)
  WHERE u.active AND (u.last_login > (now()::date + '-6 months'::interval) OR u.date_added > (now()::date + '-1 month'::interval))
),
recent_eda_users AS (
  SELECT u.user_id, u.role_id, u.user_provider_id, egpt.provider_id AS eda_user_provider_id, egpt.provider_id
  FROM recent_users u
    JOIN sp_user_eda_group ueg ON (ueg.user_id = u.user_id)
    JOIN sp_eda_group_provider_tree egpt ON (egpt.eda_group_id = ueg.eda_group_id)
)
SELECT user_access_tier || '|' || user_provider_id AS tier_link, uat.*
FROM (
  SELECT DISTINCT 1 AS user_access_tier, u.user_provider_id, p.provider_id
  FROM recent_users u
    JOIN tier1_roles r USING (role_id), 
    sp_provider p
  WHERE p.active

UNION
  SELECT DISTINCT 2 AS user_access_tier, u.user_provider_id, spt.ancestor_provider_id AS provider_id
  FROM recent_users u
    JOIN tier2_roles r USING (role_id)
    JOIN sp_provider_tree spt ON (spt.provider_id = u.user_provider_id)
UNION
  SELECT DISTINCT 3 AS user_access_tier, u.user_provider_id, spt.ancestor_provider_id AS provider_id
  FROM recent_users u
    JOIN sp_provider_tree spt ON (spt.provider_id = u.user_provider_id)
    LEFT JOIN (SELECT role_id FROM tier1_roles t1 UNION SELECT role_id FROM tier2_roles) tlr ON (u.role_id = tlr.role_id) 
  WHERE tlr.role_id IS NULL
UNION
  -- EDA Pieces
  SELECT DISTINCT 2 AS user_access_tier, uap.user_provider_id, dp.provider_id
  FROM ( SELECT DISTINCT u.user_provider_id, u.eda_user_provider_id AS provider_id
         FROM recent_eda_users u JOIN tier2_roles r USING (role_id)
  ) uap
  JOIN sp_provider_tree dp ON dp.ancestor_provider_id = uap.provider_id
  WHERE dp.ancestor_provider_id = uap.user_provider_id
UNION
  SELECT DISTINCT 2 AS user_access_tier, uap.user_provider_id, dp.provider_id
  FROM ( SELECT DISTINCT eda_user_provider_id AS user_provider_id, eda_user_provider_id AS provider_id
         FROM recent_eda_users u JOIN tier2_roles r USING (role_id) 
  ) uap
    JOIN sp_provider_tree dp ON dp.ancestor_provider_id = uap.provider_id
  WHERE dp.ancestor_provider_id = uap.user_provider_id
UNION
  SELECT DISTINCT 2 AS user_access_tier, eda_user_provider_id AS user_provider_id, spt.ancestor_provider_id AS provider_id
  FROM recent_eda_users u
    JOIN tier2_roles r USING (role_id)
    JOIN sp_provider_tree spt ON (spt.provider_id = u.user_provider_id)
UNION
  SELECT DISTINCT 3 AS user_access_tier, eda_user_provider_id AS user_provider_id, spt.ancestor_provider_id AS provider_id
  FROM recent_eda_users u
    JOIN sp_provider_tree spt ON (spt.provider_id = u.user_provider_id)
    LEFT JOIN (SELECT role_id FROM tier1_roles t1 UNION SELECT role_id FROM tier2_roles) tlr ON (u.role_id = tlr.role_id) 
  WHERE tlr.role_id IS NULL
) uat
ORDER BY 1, 2;

ALTER TABLE qlik_user_access_tier_view OWNER TO sp5user;