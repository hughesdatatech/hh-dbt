{% macro build_program_indicators(part1, part2) %}

    {{ part1 }} = 'acute' as is_acute_program,
    {{ part1 }} = 'chronic' as is_chronic_program,
    {{ part1 }} = 'perisurgical' as is_perisurgical_program,
    {{ part1 }} = 'prevention' as is_prevention_program,
    {{ part1 }} = 'expert' as is_expert_program,
    {{ part2 }} = 'pelvic' as is_pelvic_indication,
    {{ part2 }} = 'ankle' as is_ankle_indication,
    {{ part2 }} = 'wrist' as is_wrist_indication,
    {{ part2 }} = 'hand' as is_hand_indication,
    {{ part2 }} = 'elbow' as is_elbow_indication,
    {{ part2 }} = 'foot' as is_foot_indication,
    {{ part2 }} = 'balance' as is_balance_indication,
    {{ part2 }} = 'back' as is_back_indication,
    {{ part2 }} = 'whole' as is_whole_indication,
    {{ part2 }} = 'shoulder' as is_shoulder_indication,
    {{ part2 }} = 'neck' as is_neck_indication,
    {{ part2 }} = 'knee' as is_knee_indication,
    {{ part2 }} = 'hip' as is_hip_indication

{%- endmacro -%}
