{{
    flatten_json(
        model_name = source('source_arghyam_surveys', 'encounters_normalized'),
        json_column = '_airbyte_data'
    )
}}
