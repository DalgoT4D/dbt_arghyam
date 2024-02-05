{{
    flatten_json(
        model_name = source('source_arghyam_surveys', 'subjects_normalized'),
        json_column = '_airbyte_data'
    )
}}
