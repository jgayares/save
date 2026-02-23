-- DROP FUNCTION public.get_program_assets_for_grade_curriculum(uuid, text, text);

CREATE OR REPLACE FUNCTION public.get_program_assets_for_grade_curriculum(p_org_id uuid, p_curriculum_name text, p_grade_level_name text)
 RETURNS TABLE(program_id uuid, program_name text, curriculum_id uuid, curriculum_name text, program_asset_id uuid, program_asset_name text, program_asset_level text, program_asset_inclusion text[], program_asset_description text, program_asset_video text, program_asset_image text, program_asset_summary text, program_asset_logo text, metadata jsonb, program_asset_learning_material_id uuid, program_asset_learning_material_name text, payment_prerequisites jsonb, grade_level text[], is_available boolean, summary text, program_auxiliary_services text[], applicable_curriculums text[])
 LANGUAGE plpgsql
AS $function$
BEGIN
  RETURN QUERY
  
  WITH ctx AS (
    SELECT c.id, c.name
    FROM curriculums c
    WHERE c.is_active = true 
      AND c.name = p_curriculum_name
    LIMIT 1
  ),
  
  grade_ctx AS (
    SELECT gl.id 
    FROM grade_levels gl 
    WHERE gl.name = p_grade_level_name 
    LIMIT 1
  ),

  level_ranges AS (
    SELECT 
      cr.program_id, 
      cr.learning_material_id,
      (array_agg(gl.name ORDER BY gl.level ASC))[1] || ' - ' || (array_agg(gl.name ORDER BY gl.level DESC))[1] as level_display
    FROM curriculum_rules cr
    JOIN grade_levels gl ON gl.id = cr.grade_level_id
    JOIN ctx ON ctx.id = cr.curriculum_id
    WHERE cr.is_active = true 
      AND cr.organization_id = p_org_id
    GROUP BY cr.program_id, cr.learning_material_id
  ),
  
  aux_services AS (
    SELECT 
      pasr.program_id, 
      pasr.learning_material_id, 
      array_agg(pas.name) as service_names
    FROM public.program_auxiliary_service_rules pasr
    JOIN public.program_auxiliary_services pas ON pas.id = pasr.program_auxiliary_service_id
    JOIN grade_ctx gc ON pasr.grade_level_id = gc.id
    WHERE pasr.is_active = true 
      AND pas.is_active = true
      AND pasr.organization_id = p_org_id
    GROUP BY pasr.program_id, pasr.learning_material_id
  ),

  applicable_currs AS (
    SELECT 
      cr.program_id, 
      cr.learning_material_id, 
      array_agg(DISTINCT c.name ORDER BY c.name) as curr_names
    FROM curriculum_rules cr
    JOIN grade_levels gl ON gl.id = cr.grade_level_id
    JOIN curriculums c ON c.id = cr.curriculum_id
    WHERE cr.is_active = true 
      AND c.is_active = true
      AND cr.organization_id = p_org_id
      AND gl.name = p_grade_level_name
    GROUP BY cr.program_id, cr.learning_material_id
  ),
  
  rules AS (
    SELECT DISTINCT cr.program_id, cr.learning_material_id
    FROM curriculum_rules cr
    JOIN grade_levels gl ON gl.id = cr.grade_level_id
    JOIN ctx ON ctx.id = cr.curriculum_id
    WHERE cr.is_active = true 
      AND cr.organization_id = p_org_id 
      AND gl.name = p_grade_level_name
  ),
  
  required_grade_levels AS (
    SELECT
      x.program_id,
      x.learning_material_id,
      array_agg(DISTINCT x.grade_level_name ORDER BY x.grade_level_name) AS g_levels
    FROM (
      SELECT cr.program_id, cr.learning_material_id, gl.name AS grade_level_name
      FROM curriculum_rules cr
      JOIN grade_levels gl ON gl.id = cr.grade_level_id
      JOIN ctx ON ctx.id = cr.curriculum_id
      WHERE cr.is_active = true AND cr.organization_id = p_org_id
      UNION ALL
      SELECT cr.program_id, NULL::uuid, gl.name AS grade_level_name
      FROM curriculum_rules cr
      JOIN grade_levels gl ON gl.id = cr.grade_level_id
      JOIN ctx ON ctx.id = cr.curriculum_id
      WHERE cr.is_active = true AND cr.organization_id = p_org_id
    ) x
    GROUP BY x.program_id, x.learning_material_id
  ),
  
  payment_prerequisites_src AS (
    SELECT ppp.program_id, ppp.learning_material_id, 
           jsonb_build_object('type', ppp.type, 'cart_id', ppp.cart_id) AS item
    FROM public.program_payment_prerequisites ppp
    JOIN ctx ON ctx.id = ppp.curriculum_id
    LEFT JOIN grade_ctx gc ON true
    WHERE (ppp.grade_level_id IS NULL OR ppp.grade_level_id = gc.id)
  ),
  
  payment_prerequisites AS (
    SELECT s.program_id, s.learning_material_id, 
           jsonb_agg(s.item ORDER BY s.item->>'type') AS p_reqs
    FROM (SELECT DISTINCT pps.program_id, pps.learning_material_id, pps.item FROM payment_prerequisites_src pps) s
    GROUP BY s.program_id, s.learning_material_id
  )

  SELECT
    p.id,                                     
    p.name,                                   
    ctx.id,                                   
    ctx.name,                                 
    pa.id,                                    
    pa.name,                                  
    COALESCE(lr.level_display, pa.level),
    pa.inclusion,                             
    pa.description,                           
    pa.video,                                 
    pa.image,                                 
    pa.summary,                               
    pa.logo,                                  
    pa.metadata,                              
    pa.learning_material_id,                  
    lm.name,                                  
    COALESCE(ppp.p_reqs, '[]'::jsonb),        
    COALESCE(rgl.g_levels, ARRAY[]::text[]),  
    CASE
      WHEN pa.learning_material_id IS NULL THEN true
      ELSE EXISTS (
        SELECT 1 FROM rules r 
        WHERE r.program_id = pa.program_id 
          AND r.learning_material_id = pa.learning_material_id
      )
    END,                                      
    pa.summary,
    COALESCE(aux.service_names, ARRAY[]::text[]),
    COALESCE(ac.curr_names, ARRAY[]::text[]) 
  FROM program_assets pa
  JOIN programs p ON p.id = pa.program_id AND p.is_active = true
  LEFT JOIN learning_materials lm ON lm.id = pa.learning_material_id
  LEFT JOIN ctx ON true
  LEFT JOIN level_ranges lr ON lr.program_id = pa.program_id 
       AND (lr.learning_material_id = pa.learning_material_id OR (pa.learning_material_id IS NULL AND lr.learning_material_id IS NULL))
  LEFT JOIN payment_prerequisites ppp ON ppp.program_id = pa.program_id AND ppp.learning_material_id = pa.learning_material_id
  LEFT JOIN required_grade_levels rgl ON rgl.program_id = pa.program_id
       AND ((pa.learning_material_id IS NOT NULL AND rgl.learning_material_id = pa.learning_material_id)
            OR (pa.learning_material_id IS NULL AND rgl.learning_material_id IS NULL))
  LEFT JOIN aux_services aux ON aux.program_id = pa.program_id 
       AND aux.learning_material_id = pa.learning_material_id
  LEFT JOIN applicable_currs ac ON ac.program_id = pa.program_id 
       AND (ac.learning_material_id = pa.learning_material_id OR (pa.learning_material_id IS NULL AND ac.learning_material_id IS NULL))
  WHERE p.organization_id = p_org_id
  ORDER BY p.name, pa.name;
END
$function$
;
