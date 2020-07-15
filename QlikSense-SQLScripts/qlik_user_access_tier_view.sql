-- DROP VIEW IF EXISTS qlik_user_access_tier_view;
 
CREATE OR REPLACE VIEW qlik_user_access_tier_view AS 
SELECT user_access_tier || '|' || user_provider_id AS tier_link, uat.*
FROM (
  SELECT DISTINCT 1 AS user_access_tier, u.provider_id AS user_provider_id, p.provider_id
  FROM sp_user u
     JOIN (SELECT DISTINCT role_id FROM sp_action_in_role ar JOIN action a ON ar.action_id = a.action_id WHERE a.name = 'VISIBILITY_BYPASSSECURITY') r USING (role_id)
     JOIN (SELECT DISTINCT user_id
           FROM sp_boxi_license_allocation bla JOIN sp_boxi_license bl ON bl.license_id = bla.license_id JOIN boxi_license_type blt ON blt.license_type_id = bl.license_type_id
           WHERE bla.active AND bl.active AND blt.name::text = 'ART-AR'::text) bl ON u.user_id = bl.user_id
     JOIN (SELECT DISTINCT user_id
           FROM sp_user u 
             JOIN sp_provider p USING (provider_id) 
             JOIN (SELECT DISTINCT role_id, r.name FROM sp_action_in_role ar JOIN sp_role r USING (role_id) JOIN action a USING (action_id) WHERE a.name = 'CLIENT_CLIENT_VIEW') r USING (role_id)
           WHERE u.active AND p.active AND (p.enable_clientpoint 
             OR (r.name ILIKE 'READ%' AND (p.activity_flag OR p.callcenter_flag))
             OR EXISTS (SELECT 1
                        FROM sp_setting 
                        WHERE (name = 'callcenter_211module' AND p.callcenter_flag AND u.allow_callpoint) 
                           OR (name = 'activitypoint_module' AND p.activity_flag AND u.allow_activitypoint)))) cperm ON u.user_id = cperm.user_id,
     sp_provider p
  WHERE u.active AND p.active AND (u.last_login > (now()::date + '-6 months'::interval) OR u.date_added > (now()::date + '-1 month'::interval))
UNION
  SELECT DISTINCT 2 AS user_access_tier, uap.user_provider_id, dp.provider_id
  FROM ( SELECT DISTINCT u.provider_id AS user_provider_id, egpt.provider_id
         FROM sp_user_eda_group ueg
           JOIN sp_user u ON ueg.user_id = u.user_id
           JOIN sp_user_eda_group e5 ON u.user_id = e5.user_id
           JOIN sp_eda_group_provider_tree egpt ON egpt.eda_group_id = ueg.eda_group_id
           JOIN (SELECT DISTINCT role_id FROM sp_action_in_role ar JOIN action a USING (action_id) WHERE a.name = 'VISIBILITY_BYPASSSECURITY_TREE') r USING (role_id)
           JOIN (SELECT DISTINCT user_id
                 FROM sp_boxi_license_allocation bla JOIN sp_boxi_license bl ON bl.license_id = bla.license_id JOIN boxi_license_type blt ON blt.license_type_id = bl.license_type_id
                 WHERE bla.active AND bl.active AND blt.name::text = 'ART-AR'::text) bl ON u.user_id = bl.user_id
           JOIN (SELECT DISTINCT user_id
                 FROM sp_user u 
                   JOIN sp_provider p USING (provider_id) 
                   JOIN (SELECT DISTINCT role_id, r.name FROM sp_action_in_role ar JOIN sp_role r USING (role_id) JOIN action a USING (action_id) WHERE a.name = 'CLIENT_CLIENT_VIEW') r USING (role_id)
                 WHERE u.active AND p.active AND (p.enable_clientpoint 
                   OR (r.name ILIKE 'READ%' AND (p.activity_flag OR p.callcenter_flag))
                   OR EXISTS (SELECT 1
                              FROM sp_setting 
                              WHERE (name = 'callcenter_211module' AND p.callcenter_flag AND u.allow_callpoint) 
                                 OR (name = 'activitypoint_module' AND p.activity_flag AND u.allow_activitypoint)))) cperm ON u.user_id = cperm.user_id
         WHERE u.active AND (u.last_login > (now()::date + '-6 months'::interval) OR u.date_added > (now()::date + '-1 month'::interval))
  ) uap
  JOIN sp_provider_tree dp ON dp.ancestor_provider_id = uap.provider_id
  WHERE dp.ancestor_provider_id = uap.user_provider_id
UNION
  SELECT DISTINCT 2 AS user_access_tier, u.provider_id AS user_provider_id, spt.ancestor_provider_id AS provider_id
  FROM sp_user u
    JOIN (SELECT DISTINCT role_id FROM sp_action_in_role ar JOIN action a USING (action_id) WHERE a.name = 'VISIBILITY_BYPASSSECURITY_TREE') r USING (role_id)
    JOIN sp_provider_tree spt ON spt.provider_id = u.provider_id
    JOIN (SELECT DISTINCT user_id
          FROM sp_boxi_license_allocation bla JOIN sp_boxi_license bl ON bl.license_id = bla.license_id JOIN boxi_license_type blt ON blt.license_type_id = bl.license_type_id
          WHERE bla.active AND bl.active AND blt.name::text = 'ART-AR'::text) blt ON (blt.user_id = u.user_id)
    JOIN (SELECT DISTINCT user_id
          FROM sp_user u 
            JOIN sp_provider p USING (provider_id) 
            JOIN (SELECT DISTINCT role_id, r.name FROM sp_action_in_role ar JOIN sp_role r USING (role_id) JOIN action a USING (action_id) WHERE a.name = 'CLIENT_CLIENT_VIEW') r USING (role_id)
          WHERE u.active AND p.active AND (p.enable_clientpoint 
            OR (r.name ILIKE 'READ%' AND (p.activity_flag OR p.callcenter_flag))
            OR EXISTS (SELECT 1
                       FROM sp_setting 
                       WHERE (name = 'callcenter_211module' AND p.callcenter_flag AND u.allow_callpoint) 
                          OR (name = 'activitypoint_module' AND p.activity_flag AND u.allow_activitypoint)))) cperm ON u.user_id = cperm.user_id
  WHERE u.active AND (u.last_login > (now()::date + '-6 months'::interval) OR u.date_added > (now()::date + '-1 month'::interval)) 
UNION
  SELECT DISTINCT 3 AS user_access_tier, u.provider_id AS user_provider_id, spt.ancestor_provider_id AS provider_id
  FROM sp_user u
    JOIN sp_provider_tree spt ON spt.provider_id = u.provider_id
    JOIN (SELECT DISTINCT user_id
          FROM sp_boxi_license_allocation bla JOIN sp_boxi_license bl ON bl.license_id = bla.license_id JOIN boxi_license_type blt ON blt.license_type_id = bl.license_type_id
          WHERE bla.active AND bl.active AND blt.name::text = 'ART-AR'::text) blt ON (blt.user_id = u.user_id)
    JOIN (SELECT DISTINCT user_id
          FROM sp_user u 
            JOIN sp_provider p USING (provider_id) 
            JOIN (SELECT DISTINCT role_id, r.name FROM sp_action_in_role ar JOIN sp_role r USING (role_id) JOIN action a USING (action_id) WHERE a.name = 'CLIENT_CLIENT_VIEW') r USING (role_id)
          WHERE u.active AND p.active AND (p.enable_clientpoint 
            OR (r.name ILIKE 'READ%' AND (p.activity_flag OR p.callcenter_flag))
            OR EXISTS (SELECT 1
                       FROM sp_setting 
                       WHERE (name = 'callcenter_211module' AND p.callcenter_flag AND u.allow_callpoint) 
                          OR (name = 'activitypoint_module' AND p.activity_flag AND u.allow_activitypoint)))) cperm ON u.user_id = cperm.user_id
  WHERE u.active AND (u.last_login > (now()::date + '-6 months'::interval) OR u.date_added > (now()::date + '-1 month'::interval))
    AND NOT (u.user_id IN ( SELECT DISTINCT u2.user_id
                            FROM sp_user u2
                              JOIN sp_action_in_role aic2 ON aic2.role_id = u2.role_id
                              JOIN action a2 ON a2.action_id = aic2.action_id
                            WHERE u2.active 
                              AND (a2.name::text = ANY (ARRAY['VISIBILITY_BYPASSSECURITY_TREE'::character varying, 'VISIBILITY_BYPASSSECURITY'::character varying]::text[])) 
                              AND (u2.last_login > (now()::date + '-6 months'::interval) OR u2.date_added > (now()::date + '-1 month'::interval)))
    )
UNION
  SELECT DISTINCT 2 AS user_access_tier, uap.user_provider_id, dp.provider_id
  FROM ( SELECT DISTINCT egpt.provider_id AS user_provider_id, egpt.provider_id
         FROM sp_user_eda_group ueg
           JOIN sp_user u ON ueg.user_id = u.user_id
           JOIN sp_eda_group_provider_tree egpt ON egpt.eda_group_id = ueg.eda_group_id
           JOIN (SELECT DISTINCT role_id FROM sp_action_in_role ar JOIN action a USING (action_id) WHERE a.name = 'VISIBILITY_BYPASSSECURITY_TREE') r USING (role_id)
           JOIN (SELECT DISTINCT user_id
                 FROM sp_boxi_license_allocation bla JOIN sp_boxi_license bl ON bl.license_id = bla.license_id JOIN boxi_license_type blt ON blt.license_type_id = bl.license_type_id
                 WHERE bla.active AND bl.active AND blt.name::text = 'ART-AR'::text) blt ON (blt.user_id = u.user_id)
           JOIN (SELECT DISTINCT user_id
                 FROM sp_user u 
                   JOIN sp_provider p USING (provider_id) 
                   JOIN (SELECT DISTINCT role_id, r.name FROM sp_action_in_role ar JOIN sp_role r USING (role_id) JOIN action a USING (action_id) WHERE a.name = 'CLIENT_CLIENT_VIEW') r USING (role_id)
                 WHERE u.active AND p.active AND (p.enable_clientpoint 
                   OR (r.name ILIKE 'READ%' AND (p.activity_flag OR p.callcenter_flag))
                   OR EXISTS (SELECT 1
                              FROM sp_setting 
                              WHERE (name = 'callcenter_211module' AND p.callcenter_flag AND u.allow_callpoint) 
                                 OR (name = 'activitypoint_module' AND p.activity_flag AND u.allow_activitypoint)))) cperm ON u.user_id = cperm.user_id
         WHERE u.active 
           AND (u.last_login > (now()::date + '-6 months'::interval) OR u.date_added > (now()::date + '-1 month'::interval)) 
   ) uap
   JOIN sp_provider_tree dp ON dp.ancestor_provider_id = uap.provider_id
  WHERE dp.ancestor_provider_id = uap.user_provider_id
UNION
  SELECT DISTINCT 2 AS user_access_tier,
    p2.provider_id AS user_provider_id,
    spt.ancestor_provider_id AS provider_id
  FROM sp_user u
    JOIN sp_user_eda_group e2 ON u.user_id = e2.user_id
    JOIN sp_eda_group_provider_tree p2 ON e2.eda_group_id = p2.eda_group_id
    JOIN (SELECT DISTINCT role_id FROM sp_action_in_role ar JOIN action a USING (action_id) WHERE a.name = 'VISIBILITY_BYPASSSECURITY_TREE') r USING (role_id)
    JOIN sp_provider_tree spt ON spt.provider_id = u.provider_id
    JOIN (SELECT DISTINCT user_id
          FROM sp_boxi_license_allocation bla JOIN sp_boxi_license bl ON bl.license_id = bla.license_id JOIN boxi_license_type blt ON blt.license_type_id = bl.license_type_id
          WHERE bla.active AND bl.active AND blt.name::text = 'ART-AR'::text) bl ON (bl.user_id = u.user_id)
    JOIN (SELECT DISTINCT user_id
          FROM sp_user u 
            JOIN sp_provider p USING (provider_id) 
            JOIN (SELECT DISTINCT role_id, r.name FROM sp_action_in_role ar JOIN sp_role r USING (role_id) JOIN action a USING (action_id) WHERE a.name = 'CLIENT_CLIENT_VIEW') r USING (role_id)
            WHERE u.active AND p.active AND (p.enable_clientpoint 
              OR (r.name ILIKE 'READ%' AND (p.activity_flag OR p.callcenter_flag))
              OR EXISTS (SELECT 1
                         FROM sp_setting 
                         WHERE (name = 'callcenter_211module' AND p.callcenter_flag AND u.allow_callpoint) 
                            OR (name = 'activitypoint_module' AND p.activity_flag AND u.allow_activitypoint)))) cperm ON u.user_id = cperm.user_id
  WHERE u.active AND (u.last_login > (now()::date + '-6 months'::interval) OR u.date_added > (now()::date + '-1 month'::interval)) 
UNION
  SELECT DISTINCT 3 AS user_access_tier,
    p3.provider_id AS user_provider_id,
    spt.ancestor_provider_id AS provider_id
  FROM sp_user u
    JOIN sp_user_eda_group e3 ON u.user_id = e3.user_id
    JOIN sp_eda_group_provider_tree p3 ON e3.eda_group_id = p3.eda_group_id
    JOIN sp_provider_tree spt ON spt.provider_id = u.provider_id
    JOIN (SELECT DISTINCT user_id
          FROM sp_boxi_license_allocation bla JOIN sp_boxi_license bl ON bl.license_id = bla.license_id JOIN boxi_license_type blt ON blt.license_type_id = bl.license_type_id
          WHERE bla.active AND bl.active AND blt.name::text = 'ART-AR'::text) bl ON (bl.user_id = u.user_id)
    JOIN (SELECT DISTINCT user_id
          FROM sp_user u 
            JOIN sp_provider p USING (provider_id) 
            JOIN (SELECT DISTINCT role_id, r.name FROM sp_action_in_role ar JOIN sp_role r USING (role_id) JOIN action a USING (action_id) WHERE a.name = 'CLIENT_CLIENT_VIEW') r USING (role_id)
          WHERE u.active AND p.active AND (p.enable_clientpoint 
            OR (r.name ILIKE 'READ%' AND (p.activity_flag OR p.callcenter_flag))
            OR EXISTS (SELECT 1
                       FROM sp_setting 
                       WHERE (name = 'callcenter_211module' AND p.callcenter_flag AND u.allow_callpoint) 
                          OR (name = 'activitypoint_module' AND p.activity_flag AND u.allow_activitypoint)))) cperm ON u.user_id = cperm.user_id
  WHERE u.active AND (u.last_login > (now()::date + '-6 months'::interval) OR u.date_added > (now()::date + '-1 month'::interval)) 
    AND NOT (u.user_id IN ( SELECT DISTINCT u2.user_id
                            FROM sp_user u2
                              JOIN sp_action_in_role aic2 ON aic2.role_id = u2.role_id
                              JOIN action a2 ON a2.action_id = aic2.action_id
                            WHERE u2.active 
                              AND (a2.name::text = ANY (ARRAY['VISIBILITY_BYPASSSECURITY_TREE'::character varying, 'VISIBILITY_BYPASSSECURITY'::character varying]::text[])) 
                              AND (u2.last_login > (now()::date + '-6 months'::interval) OR u2.date_added > (now()::date + '-1 month'::interval))))
) uat
ORDER BY 1, 2;
