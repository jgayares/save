DROP FUNCTION public.enrollment_guard(uuid, text, text, text, uuid, uuid);


CREATE OR REPLACE FUNCTION public.enrollment_guard(
    p_parent_id uuid, 
    p_program_name text, 
    p_learning_material_name text, 
    p_curriculum_name text, 
    p_grade_level_id uuid DEFAULT NULL::uuid, 
    p_student_id uuid DEFAULT NULL::uuid
)
 RETURNS TABLE(
    condition_id uuid, 
    program_id uuid, 
    program_name text, 
    curriculum text, 
    learning_material_name text, 
    requirement_code text, 
    payment_type text, 
    cart_id text, 
    is_completed boolean, 
    completed_at timestamp with time zone, 
    extracted_student_id uuid
)
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
    SELECT 
        prc.id AS condition_id,
        p.id AS program_id,
        p.name AS program_name,
        c.name as curriculum,
        lm.name AS learning_material_name,
        r.code AS requirement_code,
        ppp.type::TEXT as payment_type,
        ppp.cart_id::TEXT,
        COALESCE(prp.is_completed, false) as is_completed,
        prp.completed_at,
        prp.student_id as extracted_student_id
    FROM programs p
    JOIN learning_materials lm ON lm.name = p_learning_material_name
    JOIN curriculums c ON c.name = p_curriculum_name
    -- Link conditions to the program/material/curriculum context
    LEFT JOIN program_requirement_conditions prc ON 
        prc.program_id = p.id 
        AND prc.learning_material_id = lm.id 
        AND prc.curriculum_id = c.id
    -- Get the requirement definition and its "is_per_student" flag
    LEFT JOIN program_requirements pr ON prc.program_requirement_id = pr.id
    LEFT JOIN requirements r ON pr.requirement_id = r.id
    -- Handle payment prerequisites
    LEFT JOIN program_payment_prerequisites ppp ON 
        ppp.program_id = p.id 
        AND ppp.learning_material_id = lm.id 
        AND ppp.curriculum_id = c.id
        AND (p_grade_level_id IS NULL OR ppp.grade_level_id = p_grade_level_id)
    LEFT JOIN parent_requirement_progress prp ON 
        prp.requirement_id = r.id 
        AND prp.parent_id = p_parent_id
        AND (
            CASE 
                -- If it IS per student, we MUST match the specific student_id
                WHEN pr.is_per_student = true THEN prp.student_id = p_student_id
                -- If it is NOT per student, we ignore student_id (family level)
                -- We ensure we don't accidentally pick up a student-specific record if a general one exists
                ELSE prp.student_id IS NULL 
            END
        )
    WHERE 
        p.name = p_program_name
        AND (p_grade_level_id IS NULL OR ppp.grade_level_id = p_grade_level_id OR ppp.id IS NULL);
END;$function$;
